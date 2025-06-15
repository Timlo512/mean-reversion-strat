import argparse
import os

def main():

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run DailyWorker for market data collection.")
    parser.add_argument("worker", type=str, help="Worker to run.")
    parser.add_argument("--env", "-e", type=str, default="dev", help="Environment to run the worker in.")
    parser.add_argument("--task_list", "-t", type=str, nargs='+', help="List of tasks to run.")
    args = parser.parse_args()
    print(args)

    os.environ['ENV'] = args.env

    # Initialize the DailyWorker with the provided task list
    mod = __import__("worker", fromlist=[args.worker], level=0)
    Worker = getattr(mod, args.worker)
    worker = Worker(task_list=args.task_list)
    worker.run()

if __name__ == "__main__":
    main()