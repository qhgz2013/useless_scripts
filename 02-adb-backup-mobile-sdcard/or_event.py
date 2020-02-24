# Version 1.1
# Proving a non-poll, non-thread based OrEvent by method override
# CHANGELOG
# * Supports event nesting and multi-binding
import threading

__all__ = ['or_event', 'OrEvent']


def _or_set(self):
    self._set()
    for _changed in self._changed:
        _changed()


def _or_clear(self):
    self._clear()
    for _changed in self._changed:
        _changed()


def _orify(e, changed_callback):
    e._set = getattr(e, '_set', e.set)
    e._clear = getattr(e, '_clear', e.clear)
    _changed = getattr(e, '_changed', [])
    _changed.append(changed_callback)
    setattr(e, '_changed', _changed)
    e.set = lambda: _or_set(e)
    e.clear = lambda: _or_clear(e)


def or_event(*events):
    new_event = threading.Event()

    def changed():
        boolean = [e.is_set() for e in events]
        if any(boolean):
            new_event.set()
        else:
            new_event.clear()
    for e in events:
        _orify(e, changed)
    changed()
    return new_event


OrEvent = or_event
