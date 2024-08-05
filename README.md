# rust-nebula

`rust-nebula` is a Rust client for developers to connect to NebulaGraph. `rust-nebula` only supports NebulaGraph which uses V3 protocol now.

## What we can achieve
NebulaGraph is composed of three services: Graph service, Meta service, and Storage service, which is an architecture that separates storage and computing.

Refer to [nebula-go](https://github.com/vesoft-inc/nebula-go) and [nebula-python](https://github.com/vesoft-inc/nebula-python), we have implemented Graph Client, Meta Client and Storage Client:

* Graph Client: It supports all nGQL query. For most users, they only need this client to finish jobs.
* Meta Client: It's used to obtain some storaged info so that Storage Client can do `scan` op.
* Storage Client: It could scan existed vertex and edge, and generally, it's prepared for large-scale data science engineering and data migration in the intranet.


## Examples

It has some examples in [examples](examples/). 

## Todo
This repo is under construction. Welcome everyone to actively participate in improving the rust client and achieving more functions!

- [ ] Make value wrapper provide comprehensive support for all data types in NebualGraph.
- [ ] A good session pool for Graph Client, perhaps Storage Client and Meta Client could also use it
- [ ] More commonly used instructions encapsulation for Graph Client, such as create tag/vertex, show tag/vertex etc.

## Reference

Part of the code in this project refers to the [bk-rs/nebula-rs](https://github.com/bk-rs/nebula-rs) and [vesoft-inc/nebula-rust](https://github.com/vesoft-inc/nebula-rust) project. Thank you for the authors' open source contribution.