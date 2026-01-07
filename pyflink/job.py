import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)

# write all the data to one file
env.set_parallelism(1)

# define the source
word_count_data = ["To be, or not to be,--that is the question:--",
                   "Be all my sins remember'd."]

ds = env.from_collection(word_count_data)

def split(line):
    yield from line.split()

# compute word count
ds = ds.flat_map(split) \
       .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
       .key_by(lambda i: i[0]) \
       .reduce(lambda i, j: (i[0], i[1] + j[1]))

ds.print()

# submit for execution
env.execute("words counting")
