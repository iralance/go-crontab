# go-crontab

## env
* etcd 
`docker run -d --name Etcd-server \
  --network app-tier \
  --publish 2379:2379 \
  --publish 2380:2380 \
  --env ALLOW_NONE_AUTHENTICATION=yes \
  --env ETCD_ADVERTISE_CLIENT_URLS=http://etcd-server:2379 \
  bitnami/etcd:latest`
* mongodb
`docker run -d -p 27017:27017 --name mongodb bitnami/mongodb:latest`
