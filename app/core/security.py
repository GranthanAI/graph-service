# Graph Service is an internal service — it is consumed by Kafka events,
# not by external clients. No user authentication is required here.
#
# JWT verification is the responsibility of the Conversation Service (Option A
# in coordinating_services.md: verify locally with the shared secret).
# Graph Service trusts its Kafka event stream, not HTTP tokens.
