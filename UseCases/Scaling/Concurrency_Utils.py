import warnings
warnings.filterwarnings('ignore')

import logging
import teradatasql
from teradataml import *
import getpass
import datetime
import time
import pandas as pd
import concurrent.futures
from time import sleep
from random import random
from threading import current_thread, get_ident, get_native_id, Event
import math

from IPython.display import display

import numpy as np




def add_workload(qry, threads, delay, **kwargs):
    '''function to create a flat list of tuples to pass to
    concurrent.futures threadpool'''
    local_profile = []
    if 'iterations' in kwargs:
        iterations = kwargs['iterations']
        duration = -1
    elif 'duration' in kwargs:
        duration = kwargs['duration']
        iterations = -1
    else:
        return('Please pass either duration or iterations')
    
    # attach a threading event to the instance
    for i in range(0, threads):
        event = Event()
        t = (qry, delay, event, iterations, duration)
        local_profile.append(t)
    
    return local_profile


def run_sql(qry, delay, event, iterations, duration, conn_info):
    try:
        thread = current_thread()
        
        print(f'Started Thread {get_native_id()} at {str(datetime.datetime.now())}, {str(iterations)} iterations, Query: "{qry[:20]}..."')
        logging.info(f'Started Thread {get_native_id()}, {str(iterations)} iterations, Query: "{qry[:20]}..."')
        
        # stagger logons:
        r = random() * conn_info['num_cons']
        logging.info(f'sleeping {str(r)} seconds before logon')
        sleep(r)
        
        with teradatasql.connect(host = conn_info['host'], 
                                 user = conn_info['username'], 
                                 password = conn_info['password'], logmech = 'TD2') as con:
            
            d = pd.read_sql(f'''SET SESSION COMPUTE GROUP {conn_info['compute_group']}''', con)
            
            h = conn_info['host']
            g = conn_info['compute_group']
            logging.info(f'Thread {get_native_id()} connected to {h}, set compute group {g}')
            
            # start open-ended loop
            i = 1
            session_start = time.time()
            end_session = False
            
            #check to see if iterations is -1
            while True:
                
                # check thread event - do we kill the thread
                if event.is_set():
                    logging.info(f'Thread {get_native_id()} Killed')
                    return f'Thread {get_native_id()} Killed'
                    break;
                
                start = time.time()
                df_temp = pd.read_sql(qry, con)
                q_t = time.time() - start
                
                logging.info(f'Thread {get_native_id()}, iteration {str(i)}, query returned {str(df_temp.shape[0])} rows in {str(q_t)} seconds')
                
                # Sleep the delay plus a bit to stagger
                sleep(random() + delay)
                
                i+=1
                # check if we need to exit the loop
                # either by iteration or duration
                if duration == -1:
                    if i >= iterations: break;
                elif iterations == -1:
                    if time.time() - session_start >= duration: break;
                else: break; # something went really wrong
                    

        logging.info(f'Thread {get_native_id()} completed {str(i)} iterations' )
        
        return f'Success: Thread {get_native_id()} finished at ' + str(datetime.datetime.now())
    
    except Exception as e:
        logging.info(f'Fail: Exception in Thread {get_native_id()}, {e}')
        return f'Fail: Exception in Thread {get_native_id()}, {e}'
