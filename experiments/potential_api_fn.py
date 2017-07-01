# api.py

@method()
def resize_image(data: bytes) -> dict:
    pass


user_registered = Event()

# implementation.py


@implements(resize_image)
def resize_image(data: bytes) -> dict:
    pass


# Pros:
#   - Appealingly simple
# Cons:
#   - IDE's won't warn about definition/implementation signatures not matching
#   - Not DRY


