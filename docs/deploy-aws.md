# AWS Single-Region Deployment (EC2 + ALB + Route 53)

Goal: the same compose stack on one EC2 box, fronted properly. This is the
minimal credible cloud story; ECS/EKS is the scaling chapter, not step one.

## Topology
```
Route 53 (A/ALIAS ridemesh.yourdomain.com)
        |
Application Load Balancer  (HTTPS :443, ACM cert)
   |  listener rules:
   |    /api/*      -> target group :8080  (gateway)
   |    /socket.io* -> target group :3002  (location-service, stickiness ON)
        |
EC2 (t3.large, Ubuntu 24.04, docker compose stack)
```

## Steps
1. **EC2**: t3.large (8 GB — the stack + Rabbit + Postgres is heavy),
   security group allowing :80/:443 from the ALB SG only, plus your IP on
   :22. Install Docker + compose plugin, clone repo, `docker compose up -d`.
2. **ALB**: two target groups (gateway-8080, ws-3002), health checks on
   `/health`. On the WS target group enable **stickiness** — Socket.IO's
   HTTP-polling handshake must land on the same node before upgrade
   (with one node it's moot, but configure it now, scale later).
3. **ACM**: request cert for your domain, attach to the HTTPS listener;
   redirect HTTP->HTTPS.
4. **Route 53**: ALIAS record to the ALB DNS name.
5. **Hardening**: move `JWT_SECRET` + DB creds to SSM Parameter Store and
   inject via an env file the deploy script renders; snapshot the pgdata
   volume; CloudWatch agent shipping docker logs.

## Scaling path (interview answer, not homework)
Gateway/matching/pricing/notification are stateless -> ASG behind the ALB.
location-service scales because the Socket.IO Redis adapter already makes
any node deliver to any client. Postgres -> RDS, Redis -> ElastiCache,
Rabbit -> Amazon MQ. Nothing in the code changes — that's what the env-var
service discovery bought.
