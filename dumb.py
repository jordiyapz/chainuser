import time
import ray
import logging
import sys

ray.init()


@ray.remote
def dumb(msg):
    # logging.basicConfig(level=logging.INFO)
    # print(msg)
    logger = logging.getLogger(__name__)

    stdout_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        f'%(asctime)s - %(levelname)s - %(message)s')
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    logger.error(msg)


ref = dumb.remote('Hello world!!!')
ray.get(ref)
# sys.stdout.flush()
print('sleeping..')
time.sleep(10)

ray.shutdown()
