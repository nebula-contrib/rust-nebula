## Prerequisites

To run these examples, check the following conditions:

- Rust development environment is installed.

- NebulaGraph is deployed. For more information, see [Deployment and installation of NebulaGraph](https://docs.nebula-graph.io/master/4.deployment-and-installation/1.resource-preparations/).
  
  **Notice**: Using docker to deploy NebulaGraph may result in that storage client and meta client couldn't access storaged and metad servers. This is normal.

- Load a test dataset. You could load it by using `nebula-console`. If you don't install `nebula-console`, see [Install NebulaGraph Console](https://docs.nebula-graph.io/master/nebula-console/). And then run 
  ```
  (root@nebula) [(none)]> :play basketballplayer;
  ```
  **Notice**: `:` at the beginning is essential
