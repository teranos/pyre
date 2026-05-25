# Example Pyre handler script
#
# The runtime provides:
#   - @watch(predicate, context=...) — fire on upstream attestations
#   - @schedule(every=N) — run periodically every N seconds
#   - upstream — the triggering attestation payload (dict or None)
#   - attest() — create downstream attestations
#   - pause_schedule(id) — pause a Pulse schedule
#   - resume_schedule(id) — resume a paused schedule
#   - delete_schedule(id) — soft-delete a schedule

@watch('data:incoming', context='my/ctx')
def process(upstream):
    """Called when a data:incoming attestation fires in my/ctx."""
    payload = upstream or {}
    subject = payload.get('subjects', ['unknown'])[0]

    attest(
        subjects=[subject],
        predicates=['data:processed'],
        contexts=['my/ctx'],
        attributes={'status': 'done'},
    )

@schedule(every=300, description='Periodic health check')
def health_check():
    """Runs every 5 minutes."""
    import datetime
    now = datetime.datetime.now().isoformat()

    attest(
        subjects=['system'],
        predicates=['health:checked'],
        contexts=['my/ctx'],
        attributes={'timestamp': now},
    )
