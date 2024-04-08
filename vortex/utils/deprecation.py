import warnings
import functools


def deprecated(entity):
    """This is a decorator which can be used to mark functions or classes as deprecated."""

    if isinstance(entity, type):
        # If the entity is a class, wrap its methods with the deprecated decorator
        for attr_name, attr_value in entity.__dict__.items():
            if callable(attr_value):
                setattr(entity, attr_name, deprecated(attr_value))

        # Issue a deprecation warning when the class itself is called
        orig_init = entity.__init__

        @functools.wraps(entity.__init__)
        def new_init(self, *args, **kwargs):
            warnings.warn(f"The {entity.__name__} class is deprecated and will be removed in a future release.",
                          category=DeprecationWarning, stacklevel=2)
            orig_init(self, *args, **kwargs)

        entity.__init__ = new_init

    elif callable(entity):
        # If the entity is a function or method, issue a warning when it is called
        @functools.wraps(entity)
        def new_func(*args, **kwargs):
            warnings.warn(f"The {entity.__name__} function is deprecated and will be removed in a future release.",
                          category=DeprecationWarning, stacklevel=2)
            return entity(*args, **kwargs)

        return new_func

    return entity
