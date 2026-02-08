# 트렌드 분석 작업 실행용 prefect 서버

기술 블로그의 rss를 기반으로 업계 트렌드를 파악하기 위한 prefect 서버입니다.

기존에 airflow로 작업하던 프로젝트를 배포 환경의 메모리 부족으로 prefect로 이전하였습니다.

prefect server, worker는 개인 라즈베리 파이에 docker compose로 배포 중입니다. (메타 db, 메시지 브로커 포함)

```mermaid
graph TD
    %% 외부 접속 계층
    subgraph TEAM_NETWORK [Team Network]
        Members(Team Members)
        Tailscale(Tailscale VPN)
    end

    %% 라즈베리 파이 계층
    subgraph RASPBERRY_PI [Raspberry Pi Docker Compose]
        Server(Prefect Server)
        Services(Prefect Services)
        Postgres(Postgres DB)
        Redis(Redis)
        Worker(Prefect Worker)
        Browser(sqlite-web Browser)
        LocalDB[(Local SQLite File)]
    end

    %% 클라우드 저장소
    subgraph CLOUD_STORAGE [Cloud Storage]
        S3(AWS S3 Bucket)
    end

    %% 연결 관계
    Members --> Tailscale
    Tailscale -->|Port 4200| Server
    Tailscale -->|Port 8080| Browser

    Server <--> Postgres
    Server <--> Redis
    Services <--> Server
    Worker <--> Server

    %% 데이터 흐름
    Worker -->|Upload Parquet| S3
    Worker -->|Insert Data| LocalDB
    Browser -->|Read Data| LocalDB
```