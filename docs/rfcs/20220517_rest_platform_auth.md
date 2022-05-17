- Feature Name: HTTP Basic Authentication for RESTful Services
- Status: draft
- Start Date: YYYY-MM-DD
- Authors: Nyalia Lui
- Issue: https://github.com/redpanda-data/redpanda/issues/3227 https://github.com/redpanda-data/redpanda/issues/1548

# Executive Summary

Enable Pandaproxy and Schema Registry users to authenticate using username/password credentials passed into an HTTP "Basic" Authentication header.

## What is being proposed

A cluster configuration property:
* `rest_require_auth` (boolean, default false).

When this property is true, REST requests to Redpanda RESTful services such as the Pandaproxy and Schema Registry will be rejected unless
the HTTP header contains a valid username and password. That username must be in the `superusers` list.

## Why (short reason)

Currently, users have two options for authentication on RESTful services: nothing or mTLS.

This causes two problems:
1. Users who have not setup mTLS or do not want to use mTLS can either refrain from using RESTful services or accept the security risk of
having unauthenticated REST requests.
2. Users who _have_ set up mTLS have no way of differentiating regular REST requests from privileged requests.

Resolving this issue simplifies using the Pandaproxy and Schema Registry in production environments.

## How (short plan)

The existing `superusers` configuration property will be used to discriminate between regular users, and users that should be permitted
to RESTful services.

The plaintext password in the HTTP Authentication header can be validated against stored scram credentials, by passing the password
through the same salt/hmac steps that we would use if setting a new password.

# Guide-level explanation
## How do we teach this?

- Explain use of `--user` and `--password` on `rpk`.
- Explain that the credentials can be the same as a Kafka (SASL) username
- Explain concept of a superuser, and how the list of superusers can be
  edited like other configuration properties (i.e. via `rpk cluster config edit`)

## What if the user locks themselves out?

In extreme circumstances, they can disable auth using
`rpk cluster config force-reset rest_require_auth` while Redpanda is offline.

In general, a super user should not be locked out unless they forget their password. For example, the Schema Registry server is always
accessible by someone in `superusers`. In the case that a naive user is locked out, admins can use one of the `superusers` to reset a
password. 


## Introducing new named concepts.

All the concepts in use here already exist:
- HTTP Basic Authentication is a standard
- The concept of a superuser already exists, we are extending its definition
  to include access to the Schema Registry.
- The concept of a SASL user account already exists.

REST Service: A seperate service, often distributed in nature, in Redpanda sub-systems that use the REST API to serve requests.


# Reference-level explanation

## Detailed design

### Auth hooks in RESTful servers

We are using seastar's built-in HTTPd framework which has the functionality to capture and represent HTTP headers. We already have
existing wrappers to extract data from the header. Therefore, we will extend those wrappers to extract the username and password.

### Runtime configuration property changes

To support dynamic updates to `superusers` and `rest_require_auth`, the values of these properties will simply not be cached anywhere.
Reading them out of the per-shard cluster configuration object at the start of each request is a cheap operation.

### Which endpoints will be authenticated?

All REST endpoints will be be subject to authentication rules.

## Interaction with other features

Currently, the other RP feature that has overlap with Basic Auth on RESTful services is `rpk`.
User must go through `rpk` to set passwords for a superuser.

TODO @NyaliaLui: What other RP features do we have? Shadow Indexing has no overlap. ACLs but
that's mentioned in future work. What about cloud?

## Secure Password Hashing

SCRAM credentials may be either sha256 or sha512 type, whereas Authentication header does not specify which to use.  For both existing
and new systems, users can supply the hashing algorithm within the authentication header along with the username and hashed password.
The RESTful service (e.g., Schema Registry) will then forward the hashing algorithm to the appropriate salt/hmac mechanisms.

## Telemetry & Observability

* WARN-level log messages on HTTP error 401 Unauthorized access
  * Close connection after N failed attempts where N is a configurable number.
* WARN-level log messages on startup when `rest_require_auth` is set but no superusers are
supplied

## Drawbacks

* If all passwords for `superusers` are lost, access to all RESTful services is lost.
* Ties REST authentication to all other features that use `superusers` to restrict access
  * One such feature is Basic Auth for the Admin API #3751
* Bare-metal users might store passwords in cleartext scripts
  * This is acceptable since the alternative, no authentication at all, is worse.
  * If users have their own secrets infrastructure, they should be using it
    to store these secrets somewhere safe.

## Rationale and Alternatives

### Alternative: do not add HTTP Basic Auth

### Alternative: use seperate credentials

It is convenient but not essential to use the credientials from the Kafka protocol with RESTful services. We could enable users
to provide their own credentials. Confluent does this with user roles and a seperate password file. See Confluent's
[Schema Registry w/ Basic Auth](https://docs.confluent.io/platform/current/security/basic-auth.html#basic-auth-sr) for details.

This design would increase the surface area of HTTP Basic Auth since we would not be able to use the existing user CRUD commands.
The required password file would also add a degree of complexity compared to the existing user/password framework known in the Kafka world.

### Alternative: remove Schema Registry authentication entirely

Currently, the only option for authentication is to use mTLS. Removing mTLS support from RESTful services has a major drawback,
no security. The consequence of this alternative is that Redpanda RESTful services would not be suitable for production environmnts.

### Alternative: only authenticate REST requests that modify data (e.g., HTTP PUT, POST, and DELETE)

Creating a read-only option for RESTful services could be worthwhile for use-cases where specific users are allowed to create schema
definitions. For example, in the banking industry only the account owner or bank teller can delete an account (done with HTTP DELETE).
A drawback to this approach is that a superuser could erase data without readers knowledge.

There are only two RESTful services current in Redpanda, the Pandaproxy and Schema Registry. The above banking use-case would apply
to the Pandaproxy since any user could produce or consume data using the proxy. For the Schema Registry, however, the majority of REST
requests are to HTTP GET metadata information. So a read-only option may be unecessary since modification requests such as PUT or POST
occur less often.

## Future work

Extend Access Control Lists (ACLs) to include RESTful services so system admins can control which users can issue read or write requests.
This also implies more interaction with `rpk` ACL setup functionality.