# Span configuration feautures

class SpanStatus:
    ok = 'OK'
    error = 'ERROR'
    unset = 'UNSET'

class SpanKind:
    client = 'CLIENT'
    server = 'SERVER'
    producer = 'PRODUCER'
    consumer = 'CONSUMER'
    internal = 'INTERNAL'
