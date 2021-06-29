# AMQP Client

## Changelog

# [6.0.0] (2021-06-29)

### Removed

- Dropped support for Node 12

### Added

- Added support for Node 16

# [4.0.0](https://gitlab.com/prismamediadigital/one/js-amqp-client/compare/3.0.6...4.0.0) (2019-03-27)

### Breaking changes

- Supports only node >= 8.9.0

# [3.0.0](https://bitbucket.org/prismamediadigital/js-amqp-client/branches/compare/3.0.0%0D2.0.3) (2017-09-27)

### Breaking changes

- Removed timeouts in order to simplify the code

# [2.0.3](https://bitbucket.org/prismamediadigital/js-amqp-client/branches/compare/2.0.3%0D2.0.2) (2017-09-10)

### Bugfix

- Fixed flow definitions

# [2.0.2](https://bitbucket.org/prismamediadigital/js-amqp-client/branches/compare/2.0.2%0D2.0.1) (2017-09-08)

### Bugfix

- Fixed resolve/reject order
- Forced onExecutionError callback to be provided

# [2.0.1](https://bitbucket.org/prismamediadigital/js-amqp-client/branches/compare/2.0.1%0D2.0.0) (2017-09-07)

### Bugfix

- Forgot to build the project

# [2.0.0](https://bitbucket.org/prismamediadigital/js-amqp-client/branches/compare/2.0.0%0D1.1.1) (2017-09-07)

### Features

- Allowed timeouts for all the consumers
- Allowed to define the error handlers

# [1.1.1](https://bitbucket.org/prismamediadigital/js-amqp-client/branches/compare/1.1.1%0D1.1.0) (2017-08-29)

### Features

- Semaphores
  - the timeouts are disabled by default.

# [1.1.0](https://bitbucket.org/prismamediadigital/js-amqp-client/branches/compare/1.1.0%0D1.0.4) (2017-08-29)

### Features

- Semaphores
  - allow to configure the following timeouts :
    - lock_timeout : time spent waiting for the "semaphore"
    - execution_timeout : time spent waiting for the end of the provided task, after the "semaphore" is acquired
