import os
import shutil
import tempfile
from multiprocessing import Process

logger = None

class Sentinel:
    pass

def write_offset_file(i, offset_file_path, modulo=500):
    if i % modulo == 0: # Only do this every so often
        temp_f = tempfile.NamedTemporaryFile(mode='wt', delete=False)
        temp_f.write(str(i))
        temp_fname = temp_f.name
        temp_f.close()
        shutil.copy(temp_fname, offset_file_path)
        os.remove(temp_fname)

def at_offset(i, start_from, f, frequency=25000):
    if i < start_from:
        if i < 10 or i % frequency == 0:
            logger.info(f'Seeking to offset...')
            return False
    return True

def set_logger(main_logger):
    global logger
    logger = main_logger

def get_start_offset(operation_list_path, ignore_checkpoint):
    start_from = 0
    offset_file_path = f'{operation_list_path}.resume'
    if not ignore_checkpoint:
        try:
            logger.info(f'Checking {offset_file_path} for a line offset...')
            with open(offset_file_path, 'r') as offset_file:
                start_from = int(offset_file.read())
        except OSError:
            pass
    return start_from, offset_file_path

def start_processes(queue, do_processing, num_procs, args):
    procs = []
    for i in range(num_procs):
        p = Process(target=do_processing, args=(i, queue, args))
        p.start()
        procs.append(p)
    return procs

def end_processes(queue, procs):
    for p in procs:
        queue.put(Sentinel())
    for p in procs:
        p.join()
