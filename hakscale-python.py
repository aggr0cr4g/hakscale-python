import os
import sys
import argparse
import logging
import redis
import yaml
import ssl
import threading
import subprocess
import time
import uuid
import boto3


# Global variables
config = None
queue_client = None


# Configuration class to hold Redis configuration
class Config:
    def __init__(self, host, port, password):
        self.redis = {
            'host': host,
            'port': port,
            'password': password
        }

# Global variables
config = None
redis_client = None

def read_lines(filename):
    with open(filename, 'r') as file:
        return file.readlines()

def check_if_all(array, comparator):
    return all(x == comparator for x in array)

def loop_through(file_slices, placeholders, command, lengths, original_lengths, test, queue_id, timeout, queue, sqs, queue_server):
    # Get the total number of combinations
    total_combinations = 1
    for length in original_lengths:
        total_combinations *= length

    # Iterate through each combination
    for index in range(total_combinations):
        line = command
        temp_index = index
        # Replace placeholders with values from file_slices
        for i, file_slice in enumerate(file_slices):
            position = temp_index % original_lengths[i]
            temp_index //= original_lengths[i]
            line = line.replace(f"_{placeholders[i]}_", file_slice[position].strip())

        # Print or push the command
        if test:
            print(f"{queue_id}:::_:::{timeout}:::_:::{line}")
            #print()
        else:
            # Inside the loop, conditionally push to AWS SQS or Redis
            if queue_server == 'aws-sqs':
                # Code to send message to AWS SQS
                response = sqs.send_message(
                    QueueUrl=queue,
                    DelaySeconds=10,
                    MessageAttributes={
                        'Command': {
                            'DataType': 'String',
                            'StringValue': line
                        }
                    },
                    MessageBody=f"{queue_id}:::_:::{timeout}:::_:::{line}"
                )
                print(f"Pushed to AWS SQS: {queue_id}:::_:::{timeout}:::_:::{line}")
            else:
                # Code to push to Redis
                redis_client.lpush(queue, f"{queue_id}:::_:::{timeout}:::_:::{line}")
                print(f"Pushed to Redis: {queue_id}:::_:::{timeout}:::_:::{line}")


def print_results(queue_id, verbose, total_combinations):
    results_received = 0
    while results_received < total_combinations:
        result = redis_client.rpop(queue_id)
        if result is None:
            if verbose:
                print("Awaiting output")
            time.sleep(1)
        else:
            print(result.decode())
            results_received += 1

def push_it(command, queue, parameters_string, test, timeout, verbose, queue_server):
    split = []
    filenames = []
    placeholders = []

    parameters = parameters_string.split(",")
    for p in parameters:
        split = p.split(":")
        placeholders.append(split[0])
        filenames.append(split[1])

    file_slices = [read_lines(filename) for filename in filenames]

    lengths = [len(file_slice) for file_slice in file_slices]
    original_lengths = lengths.copy()

    queue_id = str(uuid.uuid4())

    # Get the total number of combinations
    total_combinations = 1
    for length in original_lengths:
        total_combinations *= length

    # If queue_server is aws-sqs, use AWS SQS
    if queue_server == 'aws-sqs':
        # Load AWS SQS config
        config_path = os.path.join(os.environ['PWD'], "./awsconfig.yml")
        try:
            with open(config_path, 'r') as f:
                aws_config = yaml.safe_load(f)
        except Exception as e:
            print(f"Error opening AWS config file: {e}")
            return

        # Create SQS client
        sqs = boto3.client('sqs')
        queue_url = aws_config['sqs']['queue_url']

        # Loop through and send messages to AWS SQS
        loop_through(file_slices, placeholders, command, lengths, original_lengths, test, queue_id, timeout, queue_url, sqs, queue_server)

    # Else, use Redis
    else:
        loop_through(file_slices, placeholders, command, lengths, original_lengths, test, queue_id, timeout, queue, None, queue_server)
        if not test:
            print_results(queue_id, verbose, total_combinations)


def write_to_queue_and_print(command, queue, output):
    print(f"Output for command: {command}")
    #print(output.decode())
    redis_client.lpush(queue, output)

def shell_exec(command, verbose):
    split = command.split(":::_:::")
    queue = split[0]
    timeout = int(split[1])
    command = split[2]

    if verbose:
        print(f"Running command: {command}")

    try:
        output = subprocess.check_output(command, shell=True, timeout=timeout)
        write_to_queue_and_print(command, queue, output)
    except subprocess.TimeoutExpired:
        write_to_queue_and_print(command, queue, b"Command timed out.\n")
    except subprocess.CalledProcessError as e:
        write_to_queue_and_print(command, queue, e.output)

def do_work(wg, queue, verbose):
    while True:
        result = redis_client.rpop(queue)
        if result is None:
            if verbose:
                print("Polling for jobs.")
            time.sleep(1)
        else:
            shell_exec(result.decode(), verbose)
            wg.release()

def pop_it(threads, queue, verbose):
    wg = threading.Semaphore(0)

    for i in range(threads):
        threading.Thread(target=do_work, args=(wg, queue, verbose)).start()

    for i in range(threads):
        wg.acquire()

def main():
    # Check if there are enough arguments
    if len(sys.argv) < 2:
        print("Error: Subcommand missing or incorrect.")
        return

    # Argument parsing
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subcommand')

    # Common argument for specifying the queue server
    parser.add_argument('--queue-server', type=str, default='redis', choices=['redis', 'aws-sqs'], help='Specify the queue server to use (default: redis)')


    # Push subcommand
    parser_push = subparsers.add_parser('push')
    parser_push.add_argument('-v', '--verbose', action='store_true', help='verbose mode')
    parser_push.add_argument('-c', '--command', type=str, help='the command you wish to scale, including placeholders')
    parser_push.add_argument('-q', '--queue', type=str, default='cmd', help='the name of the queue that you would like to push jobs to')
    parser_push.add_argument('-p', '--parameters', type=str, help='the placeholders and files being used')
    parser_push.add_argument('--test', action='store_true', help="print the commands to terminal, don't actually push them to redis")
    parser_push.add_argument('-t', '--timeout', type=int, default=0, help='timeout for the commands (in seconds)')

    # Pop subcommand
    parser_pop = subparsers.add_parser('pop')
    parser_pop.add_argument('-v', '--verbose', action='store_true', help='verbose mode')
    parser_pop.add_argument('-q', '--queue', type=str, default='cmd', help='the name of the queue that you would like to pop jobs from')
    parser_pop.add_argument('-t', '--threads', type=int, default=5, help='number of threads')

    # Parsearguments
    args = parser.parse_args()

    # Determine which queue server to use
    if args.queue_server == 'redis':
        # Load config file for Redis
        config_path = os.path.join(os.environ['PWD'], "./redisconfig.yml")
        try:
            with open(config_path, 'r') as f:
                global config
                config = yaml.safe_load(f)
        except Exception as e:
            print(f"Error opening config file: {e}")
            return

        # Connect to Redis server
        global queue_client
        queue_client = redis.Redis(
            host=config['redis']['host'],
            port=config['redis']['port'],
            password=config['redis']['password'],
            ssl=False,
            ssl_cert_reqs=ssl.CERT_NONE
        )
        # Check Redis server connection
        try:
            queue_client.ping()
            print("Connected to Redis server")
        except redis.ConnectionError as e:
            print(f"Unable to connect to specified Redis server: {e}")
            sys.exit(1)
    elif args.queue_server == 'aws-sqs':
        # Connect to AWS SQS here
        # Check Redis server connection
        print("Using AWS SQS")
        # implement SQS logic in punctions.

    # Handle subcommands
    if args.subcommand == 'push':
        if args.timeout == 0:
            logging.error("You must specify a timeout to avoid leaving your workers endlessly working. Hint: -t <seconds>")
            sys.exit(1)
        push_it(args.command, args.queue, args.parameters, args.test, args.timeout, args.verbose, args.queue_server)

    elif args.subcommand == 'pop':
        pop_it(args.threads, args.queue, args.verbose)

    else:
        print("Error: Subcommand missing or incorrect.")
        sys.exit(1)

if __name__ == "__main__":
    main()
