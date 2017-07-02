# Example use of API once it has been defined


from mycompany.common.auth import api

# Blocking calls
user_info = api.get_user(username='testuser')
password_ok = api.check_passsword(password='Passw0rd1')
api.user_registered.listen(my_callback)

# Parallel calls
async_result1 = api.get_user.async(username='testuser')
async_result2 = api.check_passsword.async(password='Passw0rd1')
user_info = async_result1.wait()
password_ok = async_result2.wait()


# Pro
#   - Presence of 'api' indicates that this is an external service
#   - As api is an object, one cannot access the methods/events without the prefix
#   - Simple, readable
# Con
#   - Use of 'api' is by convention only
#       - ...but we're all consenting adults.


# Developer tools


api.debug.enable()
api.debug.disable()
# Enables/disables debugging for calls using this api instance

api.debug.info('get_user')
# Shows: number of consumers, consumer information, last call timestamp, last call args, last call response,
# last handled by

api.debug.trace('get_user', username='testuser')
# Shows:
#     - "WARNING: Only works when working with other warren consumers & producers"
#     - Total number of handlers listening for this api call/event [1]
#     - Raw message as sent to broker
#     - Expecting response to tcp://10.1.2.3:54142
#     - Raw message received by rabbitmq [1]
#     - "Waiting for debug information from handlers..." [3]
#     - Consumer ---> Message received from by PID at HOST
#     - Consumer ---> Raw message as received from broker: ...
#     - Consumer ---> Message being handled by implementation mycompany.common.auth.AuthApi.get_user()
#     - Consumer ---> Implementation returned result: ...
#     - Consumer ---> Returning result to tcp://10.1.2.3:54142
#     - Consumer ---> Acknowledgement sent to broker
#     - Consumer ---> All done
#     - Response received from consumer: ...
#     - Done

# [1] Requires a debug back chanel of some sort (i.e. a debug exchange)
# [2] Creates a temporary queue on the relevant exchange.
# [3] The debugger sends the message with a 'debug-to' header, which logs back
#     via ZeroMQ
