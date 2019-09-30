import datetime
from dateutil import relativedelta

import gc

_scope_handler = {
    'date': datetime.date,
    'datetime': datetime.datetime,
    'old_datetime': datetime.datetime,
    'old_date': datetime.date}


def set_static_date_time(year=2000, month=7, day=12, hour=9, minute=47, second=40, microsecond=303620):
    global _scope_handler
    default_d = f'datetime.date({year}, {month}, {day})'
    default_dt = f'datetime.datetime({year}, {month}, {day}, {hour}, {minute}, {second}, {microsecond})'
    static_classes = f"""
import datetime
class StaticDatetime(datetime.datetime):
    @classmethod
    def now(cls, **kwargs):
        return {default_dt}
    @classmethod
    def date(cls, **kwargs):
        return StaticDate(self.year, self.month, self.day)
    @classmethod
    def __instancecheck__(cls, instance):
        if cls is StaticDate:
            return True
        else:
            return isinstance(instance, cls)
    
class StaticDate(datetime.date):
    @classmethod
    def today(cls):
        return {default_d}"""
    scope = dict()
    exec(static_classes, scope)
    perform_override(scope['StaticDate'], _scope_handler['old_date'], scope)
    perform_override(scope['StaticDatetime'], _scope_handler['old_datetime'], scope)
    _scope_handler.update({'date': scope['StaticDate'], 'datetime': scope['StaticDatetime']})


# Exception raised when a GC reference for a base class being overridden is of a type where override logic is not known
class UnknownReferenceTypeForOverrideException(Exception):
    pass


def unoverride_datetime():
    perform_override(_scope_handler['old_date'], _scope_handler['date'], dict())
    perform_override(_scope_handler['old_datetime'], _scope_handler['datetime'], dict())


def perform_override(override_class, base_class, scope):
    for referrer in gc.get_referrers(base_class):
        # Check to see if the referrer is mutable (otherwise performing an override won't do anything -
        # any immutable object with a reference will not be overridden.
        # TODO: and recursive override logic to handle referrers nested in immutable objects
        if getattr(referrer, '__hash__', None) is None:
            # If the referrer is a dict, then the reference is present as a value in the dict
            if referrer.__class__ is dict:
                # iterate over each key in the referrer
                for k in list(referrer.keys()):
                    if k in {'old_datetime', 'old_date'}:
                        continue
                    if referrer is scope:
                        continue
                    # check to see if the value associated with that key is the base class
                    if referrer[k] is base_class:
                        # if it is, then re-associate the key with the the override class
                        referrer[k] = override_class
            elif isinstance(referrer, list) and base_class in referrer:
                for idx, item in referrer:
                    if item is base_class:
                        referrer[idx] = override_class
            # All other mutable types not caught above have not had the overrides implemented,
            # so raise an Exception to alert of this fact
            else:
                print('%s' % UnknownReferenceTypeForOverrideException(
                    (f"WARNING: Found a hashable object of type {type(referrer)} "
                     f"referring to {base_class} "
                     f"while performing overrides for {override_class} "
                     f"please implement logic for handling overriding references from this type.")
                ))
