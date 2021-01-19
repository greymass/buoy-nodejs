# buoy

POST -> WebSocket forwarder

## Usage

-   POST to any data to `/<uuid>`
-   UPGRADE a WebSocket on `/<uuid>` to receive data

### Headers

`X-Buoy-Delivery` will be set to `delivered` if the data was delivered directly to a listening socket, otherwise `buffered`.

`X-Buoy-Wait` can be specified in number of seconds to wait for the data to be delivered to a listener, will return a `408` if the data cannot be delivered within time limit. Will always have `X-Buoy-Delivery: delivered` on a successful request.

`X-Buoy-Soft-Wait` can be specified in number of seconds to wait for the data to be delivered to a listener, will return a `202` if the data cannot be delivered within time limit.

## Run with docker

```
$ docker build .
...
<container id>
$ docker run -d --name buoy <container id>
```
