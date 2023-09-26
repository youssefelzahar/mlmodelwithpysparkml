

from pyhive import hive
from TCLIService.ttypes import TOperationState
cursor = hive.connect('localhost',"10000","default").cursor()
cursor.execute('SELECT * FROM data ', async_=True)

status = cursor.poll().operationState
while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
    logs = cursor.fetch_logs()
    for message in logs:
        print (message)

    # If needed, an asynchronous query can be cancelled at any time with:
    # cursor.cancel()

    status = cursor.poll().operationState


