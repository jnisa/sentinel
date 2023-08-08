# Span configuration feautures

class SpanStatus:
    OK = 'OK'
    ERROR = 'ERROR'
    UNSET = 'UNSET'

class SpanKind:
    CLIENT = 'CLIENT'
    SERVER = 'SERVER'
    PRODUCER = 'PRODUCER'
    CONSUMER = 'CONSUMER'
    INTERNAL = 'INTERNAL'
