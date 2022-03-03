# Multithreaded example program
# Takes input of a text file, and uses n threads to print the lines of that file (in whatever order)
# Brandon White, 2022

import argparse
import threading

def do_processing(tid, files, args):
    for f in files:
        print(f'(tid {tid})f: {f}')

def get_file_queues(num_threads, f):
    # Splits the list of files into (almost) equal buckets of work per thread
    queues = [ [] for i in range(num_threads) ]
    i = 0
    for filename in f:
        which_queue = i % num_threads
        queues[which_queue].append(filename.strip())
        i += 1
    return queues

def get_program_arguments():
    parser = argparse.ArgumentParser(description="Use n threads to print lines from a file.")
    parser.add_argument('filelist', help='Text file with lines to be printed.')
    parser.add_argument('--num-threads', type=int, default=1, help='Number of threads to divy up lines .')
    args = parser.parse_args()
    return args

def main():
    args = get_program_arguments()
    threads = []
    with open(args.filelist) as f: # Obtain the work distribution and hand it to the threads
        file_queues =  get_file_queues(args.num_threads, f)
        for i in range(args.num_threads):
            t = threading.Thread(target=do_processing, args=(i, file_queues[i], args))
            t.start()
            threads.append(t)
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
