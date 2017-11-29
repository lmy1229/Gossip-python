import logging

class EventListener():
    def listen(self, event_type, event):
        raise Exception("Not Implemented")


class EventEmitter():
    def __init__(self):

        self.listeners = {}

    def has_listener(self, event_type, listener):
        if event_type in self.listeners:
            if listener in self.listeners[event_type]:
                return True
        return False

    def register(self, event_type, listener):
        if not self.has_listener(event_type, listener):
            if event_type not in self.listeners:
                self.listeners[event_type] = []
            self.listeners[event_type].append(listener)
        else:
            logging.warning("EventEmitter: event %s already registered." % event_type)

    def deregister(self, event_type, listener):
        if self.has_listener(event_type, listener):
            listeners = self.listeners[event_type]
            if len(listeners) == 1:
                del self.listeners[event_type]
            else:
                listeners.remove(listener)
                self.listeners[event_type] = listeners
        else:
            logging.warning("EventEmitter: event %s is not registered." % event_type)

    def emit(self, event_type, event):
        if event_type in self.listeners:
            for listener in self.listeners[event_type]:
                listener.listen(event_type, event)
