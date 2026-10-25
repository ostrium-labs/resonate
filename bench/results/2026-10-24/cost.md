| Workload | RU / workflow | Stored KiB / workflow | Free 50M RU covers (workflows/month) | Starter $ per 1M workflows (us-east-1 … sa-east-1) |
|---|---:|---:|---:|---|
| fanout | 737 | 3.6 | 67,802 | $73.74 – $117.99 |
| chain | 1,564 | 4.3 | 31,975 | $156.37 – $250.19 |
| burst | 150 | 0.8 | 332,258 | $15.05 – $24.08 |

| Workload | TiKV deployment | $ / month | Break-even vs Starter us-east-1 (workflows/month) | vs Starter Singapore | TiKV capacity at the measured request rate (workflows/month) |
|---|---|---:|---:|---:|---:|
| fanout | aws (us-east-1, t3.large + 50 GB gp3) ×3 | 194 | 2,701,349 | 2,262,424 | 317,949,773 |
| fanout | gcp (us-central1, e2-standard-2 + 50 GB pd-balanced) ×3 | 162 | 2,261,272 | 1,895,694 | 317,949,773 |
| fanout | azure (East US, D2s v5 + E6 64 GiB) ×3 | 225 | 3,114,020 | 2,606,317 | 317,949,773 |
| fanout | single VM, aws t3.large + 50 GB gp3 (no replication, not HA) | 65 | 945,651 | 799,343 | 317,949,773 |
| chain | aws (us-east-1, t3.large + 50 GB gp3) ×3 | 194 | 1,273,954 | 1,066,958 | 212,123,644 |
| chain | gcp (us-central1, e2-standard-2 + 50 GB pd-balanced) ×3 | 162 | 1,066,414 | 894,008 | 212,123,644 |
| chain | azure (East US, D2s v5 + E6 64 GiB) ×3 | 225 | 1,468,570 | 1,229,138 | 212,123,644 |
| chain | single VM, aws t3.large + 50 GB gp3 (no replication, not HA) | 65 | 445,968 | 376,970 | 212,123,644 |
| burst | aws (us-east-1, t3.large + 50 GB gp3) ×3 | 194 | 13,237,672 | 11,086,770 | 1,430,773,979 |
| burst | gcp (us-central1, e2-standard-2 + 50 GB pd-balanced) ×3 | 162 | 11,081,122 | 9,289,645 | 1,430,773,979 |
| burst | azure (East US, D2s v5 + E6 64 GiB) ×3 | 225 | 15,259,924 | 12,771,980 | 1,430,773,979 |
| burst | single VM, aws t3.large + 50 GB gp3 (no replication, not HA) | 65 | 4,634,062 | 3,917,095 | 1,430,773,979 |

TiKV capacity basis: 2,178 protocol requests/s (the best raw-Resonate-on-TiKV run, one laptop node); requests per workflow: burst 4.0, chain 27.0, fanout 18.0.
