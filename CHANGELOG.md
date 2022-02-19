# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.4.0 - 2022-02-20
### Added
- Support for `bzip2` and `lz4` compression ([#3])

### Changed
- Switch from the unmaintained `mmap` crate to `mmap2` ([#3])
- Bump MSRV to 1.56 and edition to 2021 ([#4])

### Fixed
- Minimal versions build ([#4])

[#3]: https://github.com/SkoltechRobotics/rosbag-rs/pull/3
[#4]: https://github.com/SkoltechRobotics/rosbag-rs/pull/4
