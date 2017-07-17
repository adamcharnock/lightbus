

@method(api='my_company.auth')
def check_password(username, password):
    return username == 'admin' and password == 'secret'

user_registered = Event(api='my_company.auth', name='user_registered')

# OR

class AuthApi(Api):
    user_registered = Event()
    any = Event(matches='.#')  # Matches routing keys 'my_company.auth.#'

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


# In either case you can then call

warren.my_company.auth.check_password(username='admin', password='secret')
warren.my_company.auth.user_registered.listen(my_callable)
