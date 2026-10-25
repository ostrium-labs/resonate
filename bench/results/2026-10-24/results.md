| Workload | System | Backend | Params | OK/started | wf/s | p50 ms | p95 ms | p99 ms | Step p50/p99 ms | Sleep late p50/p99 ms | Errors | Retries | CPU avg % (sum) | RSS max MB (sum) | Host load1 start→end |
|---|---|---|---|---|---:|---:|---:|---:|---|---|---:|---:|---:|---:|---|
| burst | airflow | postgres | concurrency=500 n=500 poll_ms=100 | 22/500 | 0.05 | 475671 | 475699 | 475701 | 402997.6 / 403591.8 | – | 1112 | 0 | 92 | 4728 | 33.05→16.54 |
| burst | airflow | postgres | concurrency=500 n=500 poll_ms=100 | 105/500 | 0.20 | 519501 | 520424 | 520501 | 359883.4 / 369547.3 | – | 5031 | 1185 | 107 | 5350 | 57.69→24.17 |
| burst | airflow | postgres | concurrency=100 n=100 poll_ms=100 | 92/100 | 0.06 | 1275465 | 1289347 | 1320024 | 425858.4 / 434054.5 | – | 2740 | 0 | 58 | 5580 | 21.79→19.26 |
| burst | airflow | postgres | concurrency=500 n=500 poll_ms=1000 | 64/500 | 0.15 | 389849 | 421564 | 421569 | 343945.8 / 346824.8 | – | 2537 | 1308 | 141 | 5641 | 37.59→50.57 |
| burst | loams-durable | tikv | concurrency=500 n=500 wake="push" | 500/500 | 124.45 | 1857 | 3760 | 3840 | 161.7 / 1262.8 | – | 0 | 0 | 354 | 939 | 20.94→19.95 |
| burst | loams-durable | tikv | concurrency=500 n=500 wake="push" | 500/500 | 113.15 | 3081 | 4223 | 4298 | 311.0 / 1031.6 | – | 0 | 0 | 278 | 1297 | 18.86→20.53 |
| burst | loams-durable | tikv | concurrency=500 n=500 wake="push" | 500/500 | 129.80 | 2630 | 3726 | 3824 | 472.3 / 635.9 | – | 0 | 0 | 430 | 876 | 98.90→165.34 |
| burst | resonate | postgres | concurrency=500 n=500 wake="push" | 500/500 | 75.22 | 5104 | 6523 | 6607 | 2429.5 / 2630.8 | – | 0 | 0 | 214 | 788 | 26.27→28.51 |
| burst | resonate | postgres | concurrency=500 n=500 wake="push" | 500/500 | 114.70 | 3233 | 4231 | 4311 | 1169.9 / 1356.5 | – | 0 | 0 | 277 | 795 | 25.12→26.66 |
| burst | resonate | postgres | concurrency=500 n=500 wake="push" | 500/500 | 82.54 | 4830 | 5911 | 6024 | 2969.4 / 3657.1 | – | 0 | 0 | 297 | 807 | 27.06→38.38 |
| burst | resonate | sqlite | concurrency=500 n=500 wake="push" | 500/500 | 19.33 | 25199 | 25760 | 25844 | 18245.6 / 22495.1 | – | 0 | 0 | 24 | 483 | 18.40→22.03 |
| burst | resonate | sqlite | concurrency=500 n=500 wake="push" | 500/500 | 184.22 | 2188 | 2635 | 2677 | 584.8 / 1383.4 | – | 0 | 0 | 98 | 496 | 22.15→21.90 |
| burst | resonate | sqlite | concurrency=500 n=500 wake="push" | 500/500 | 36.62 | 12588 | 13564 | 13638 | 2281.6 / 9889.7 | – | 0 | 0 | 118 | 503 | 33.30→30.44 |
| burst | resonate | tidb-alicloud-ap-southeast-1 | concurrency=50 n=50 wake="push" | 50/50 | 2.59 | 16050 | 18704 | 19319 | 8402.3 / 9828.7 | – | 0 | 0 | 1 | 20 | 0.86→0.70 |
| burst | resonate | tidb-alicloud-ap-southeast-1 | concurrency=50 n=50 wake="push" | 50/50 | 2.37 | 18814 | 20861 | 21071 | 9001.0 / 11563.7 | – | 0 | 0 | 2 | 28 | 44.88→38.96 |
| burst | resonate | tidb-aws-ap-northeast-1 | concurrency=50 n=50 wake="push" | 50/50 | 1.08 | 29309 | 35068 | 46127 | 15511.4 / 19792.4 | – | 0 | 0 | 1 | 23 | 42.57→52.61 |
| burst | resonate | tidb-aws-ap-northeast-1 | concurrency=50 n=50 wake="push" | 50/50 | 2.34 | 19128 | 20759 | 21374 | 8402.2 / 11841.0 | – | 0 | 0 | 2 | 26 | 20.37→21.51 |
| burst | resonate | tidb-aws-ap-southeast-1 | concurrency=50 n=50 wake="push" | 50/50 | 1.71 | 24818 | 28873 | 29277 | 11358.0 / 14737.2 | – | 0 | 0 | 1 | 22 | 13.39→10.87 |
| burst | resonate | tidb-aws-ap-southeast-1 | concurrency=50 n=50 wake="push" | 50/50 | 1.25 | 36124 | 39993 | 39996 | 18735.7 / 22678.3 | – | 0 | 0 | 1 | 28 | 27.97→27.43 |
| burst | resonate | tidb-aws-eu-central-1 | concurrency=50 n=50 wake="push" | 50/50 | 1.15 | 30054 | 42945 | 43559 | 15577.4 / 20542.6 | – | 0 | 0 | 1 | 21 | 12.59→10.28 |
| burst | resonate | tidb-aws-eu-central-1 | concurrency=50 n=50 wake="push" | 18/50 | 0.14 | 121019 | 124093 | 124526 | 59580.3 / 65046.0 | – | 75 | 0 | 1 | 26 | 23.48→19.95 |
| burst | resonate | tidb-aws-sa-east-1 | concurrency=50 n=50 wake="push" | 50/50 | 1.07 | 41144 | 46674 | 46674 | 18841.8 / 22330.4 | – | 0 | 0 | 1 | 34 | 0.77→0.76 |
| burst | resonate | tidb-aws-sa-east-1 | concurrency=50 n=50 wake="push" | 50/50 | 0.87 | 47929 | 56706 | 57181 | 23172.1 / 28678.8 | – | 0 | 0 | 1 | 26 | 19.47→17.72 |
| burst | resonate | tidb-aws-us-east-1 | concurrency=50 n=50 wake="push" | 50/50 | 1.11 | 39530 | 44847 | 44847 | 19252.7 / 22060.2 | – | 0 | 0 | 1 | 21 | 2.11→2.52 |
| burst | resonate | tidb-aws-us-east-1 | concurrency=50 n=50 wake="push" | 50/50 | 0.99 | 44141 | 50152 | 50393 | 19354.7 / 25420.5 | – | 0 | 0 | 1 | 28 | 57.72→44.03 |
| burst | resonate | tidb-aws-us-west-2 | concurrency=50 n=50 wake="push" | 50/50 | 1.07 | 37807 | 43645 | 46923 | 17832.4 / 25324.6 | – | 0 | 0 | 1 | 28 | 0.60→0.56 |
| burst | resonate | tidb-aws-us-west-2 | concurrency=50 n=50 wake="push" | 50/50 | 1.10 | 30712 | 37265 | 45300 | 14948.4 / 19489.8 | – | 0 | 0 | 1 | 26 | 52.51→40.85 |
| burst | resonate | tidb-local | concurrency=100 n=100 wake="push" | 100/100 | 40.59 | 2363 | 2454 | 2459 | 730.2 / 942.3 | – | 0 | 0 | 13 | 26 | 27.79→27.09 |
| burst | resonate | tidb-local | concurrency=100 n=100 wake="push" | 100/100 | 118.56 | 710 | 830 | 842 | 346.7 / 394.8 | – | 0 | 0 | 14 | 27 | 16.69→16.69 |
| burst | resonate | tikv | concurrency=500 n=500 wake="push" | 500/500 | 62.48 | 6277 | 7264 | 7989 | 2104.7 / 3072.8 | – | 0 | 0 | 279 | 976 | 19.39→19.07 |
| burst | resonate | tikv | concurrency=500 n=500 wake="push" | 500/500 | 135.51 | 1596 | 3528 | 3655 | 172.2 / 1501.0 | – | 0 | 0 | 288 | 1871 | 14.28→14.42 |
| burst | resonate | tikv | concurrency=500 n=500 wake="push" | 500/500 | 146.99 | 2120 | 3214 | 3320 | 214.0 / 522.3 | – | 0 | 0 | 331 | 802 | 39.79→39.22 |
| burst | resonate | tikv | concurrency=500 n=500 wake="push" | 500/500 | 139.54 | 2379 | 3444 | 3530 | 700.8 / 1573.6 | – | 0 | 0 | 347 | 868 | 28.51→29.87 |
| burst | resonate | tikv | concurrency=500 n=500 wake="push" | 500/500 | 148.50 | 2179 | 3269 | 3351 | 199.2 / 373.9 | – | 0 | 0 | 421 | 849 | 29.30→28.87 |
| burst | resonate | tikv | concurrency=500 n=500 wake="push" | 500/500 | 123.39 | 2681 | 3862 | 3977 | 764.2 / 1412.2 | – | 0 | 0 | 410 | 822 | 146.61→128.43 |
| burst | temporal | postgres | concurrency=500 n=500 | 500/500 | 22.71 | 12619 | 20809 | 21212 | 68.9 / 873.1 | – | 0 | 0 | 117 | 714 | 21.62→21.35 |
| burst | temporal | postgres | concurrency=500 n=500 | 500/500 | 37.62 | 7888 | 11405 | 11775 | 32.7 / 511.1 | – | 0 | 0 | 181 | 751 | 22.29→20.99 |
| burst | temporal | postgres | concurrency=500 n=500 | 500/500 | 21.42 | 13840 | 21634 | 22727 | 65.2 / 938.8 | – | 0 | 0 | 104 | 737 | 118.86→85.42 |
| burst | temporal | postgres | concurrency=500 n=500 | 500/500 | 8.01 | 25707 | 46199 | 50310 | 188.9 / 1314.5 | – | 0 | 0 | 42 | 832 | 27.99→74.25 |
| burst | temporal-gateway | resonate-postgres | concurrency=500 n=500 | 0/0 | 0.00 | 0 | 0 | 0 | 24061.1 / 31077.3 | – | 0 | 0 | 200 | 321 | 15.00→14.60 |
| chain | airflow | postgres | concurrency=20 l=5 n=100 poll_ms=100 sleep_ms=1000 | 100/100 | 0.38 | 47880 | 67656 | 68157 | 2434.8 / 12269.0 | 2773.0 / 6695.1 | 0 | 0 | 621 | 5332 | 38.10→34.62 |
| chain | airflow | postgres | concurrency=20 l=5 n=100 poll_ms=100 sleep_ms=1000 | 100/100 | 0.35 | 50516 | 70116 | 72379 | 2908.3 / 6638.0 | 3301.2 / 6712.1 | 0 | 0 | 589 | 4436 | 22.06→57.69 |
| chain | airflow | postgres | concurrency=20 l=5 n=100 poll_ms=1000 sleep_ms=1000 | 100/100 | 0.36 | 43143 | 114369 | 115769 | 1762.4 / 16318.9 | 2385.7 / 6069.8 | 0 | 0 | 349 | 5272 | 34.87→37.59 |
| chain | loams-durable | tikv | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 0.46 | 44754 | 75630 | 113180 | 176.4 / 1764.6 | 3775.2 / 27620.6 | 0 | 0 | 16 | 543 | 13.45→20.94 |
| chain | loams-durable | tikv | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 2.39 | 7222 | 12100 | 12177 | 31.7 / 251.8 | 250.6 / 3167.6 | 0 | 0 | 77 | 2020 | 24.28→18.86 |
| chain | loams-durable | tikv | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 0.53 | 33300 | 59279 | 76557 | 113.3 / 1118.5 | 4793.3 / 23048.1 | 0 | 0 | 17 | 1589 | 179.75→98.90 |
| chain | resonate | postgres | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 1.50 | 13505 | 18316 | 18562 | 145.8 / 1864.8 | 969.8 / 7411.6 | 0 | 0 | 16 | 245 | 22.17→37.72 |
| chain | resonate | postgres | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 1.37 | 7323 | 18036 | 18884 | 72.3 / 1356.2 | 357.8 / 7164.3 | 0 | 0 | 35 | 484 | 27.97→25.12 |
| chain | resonate | postgres | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 3.02 | 6224 | 7527 | 7863 | 40.4 / 408.6 | 114.3 / 810.9 | 0 | 0 | 64 | 433 | 30.67→27.06 |
| chain | resonate | sqlite | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 2.65 | 6037 | 11772 | 12225 | 5.3 / 603.3 | 26.6 / 3403.7 | 0 | 0 | 10 | 61 | 20.54→22.17 |
| chain | resonate | sqlite | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 2.31 | 6557 | 12601 | 12964 | 4.5 / 1294.5 | 17.8 / 6954.0 | 0 | 0 | 19 | 84 | 18.29→22.15 |
| chain | resonate | sqlite | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 1.25 | 14352 | 34547 | 38674 | 7.1 / 2200.4 | 333.5 / 16936.7 | 0 | 0 | 8 | 101 | 39.68→33.30 |
| chain | resonate | tidb-alicloud-ap-southeast-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.15 | 64106 | 68833 | 69597 | 5118.8 / 5939.2 | 4954.7 / 8502.9 | 0 | 0 | 1 | 19 | 0.52→0.86 |
| chain | resonate | tidb-alicloud-ap-southeast-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.13 | 74255 | 83486 | 84916 | 5732.1 / 8800.6 | 5759.1 / 12085.5 | 0 | 0 | 1 | 21 | 28.04→44.88 |
| chain | resonate | tidb-aws-ap-northeast-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.04 | 226621 | 276702 | 293747 | 12571.8 / 50185.9 | 18219.2 / 60059.5 | 0 | 0 | 0 | 19 | 29.14→42.57 |
| chain | resonate | tidb-aws-ap-northeast-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.14 | 68604 | 69522 | 74989 | 5531.7 / 6634.4 | 5359.3 / 8215.2 | 0 | 0 | 1 | 21 | 18.20→20.14 |
| chain | resonate | tidb-aws-ap-southeast-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.13 | 76767 | 82460 | 82472 | 5637.2 / 8902.6 | 5625.4 / 10642.2 | 0 | 0 | 1 | 17 | 10.71→13.39 |
| chain | resonate | tidb-aws-ap-southeast-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.14 | 66792 | 71878 | 74554 | 5119.1 / 6658.7 | 5667.2 / 9762.9 | 0 | 0 | 1 | 22 | 24.17→27.97 |
| chain | resonate | tidb-aws-eu-central-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.07 | 135910 | 151885 | 153341 | 10844.8 / 20960.2 | 11111.7 / 19896.8 | 0 | 0 | 0 | 18 | 14.04→12.59 |
| chain | resonate | tidb-aws-eu-central-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.09 | 100119 | 109000 | 114190 | 7135.2 / 9405.3 | 7541.5 / 17440.5 | 0 | 0 | 1 | 20 | 26.25→23.48 |
| chain | resonate | tidb-aws-sa-east-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.07 | 147036 | 154035 | 159005 | 11475.5 / 13523.0 | 12314.6 / 19691.6 | 0 | 0 | 0 | 31 | 0.55→0.77 |
| chain | resonate | tidb-aws-sa-east-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.06 | 164261 | 175901 | 177875 | 12354.2 / 15616.6 | 14962.2 / 29942.7 | 0 | 0 | 0 | 21 | 28.65→19.47 |
| chain | resonate | tidb-aws-us-east-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.06 | 150016 | 163453 | 163834 | 12281.0 / 13871.3 | 13539.5 / 19775.7 | 0 | 0 | 0 | 18 | 1.54→2.11 |
| chain | resonate | tidb-aws-us-east-1 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.04 | 280030 | 292243 | 293087 | 13768.5 / 37174.1 | 23401.3 / 104761.2 | 3 | 2 | 0 | 21 | 92.66→57.72 |
| chain | resonate | tidb-aws-us-west-2 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.07 | 147337 | 158939 | 159404 | 10964.6 / 14736.2 | 12115.3 / 21946.9 | 0 | 0 | 0 | 31 | 0.99→0.60 |
| chain | resonate | tidb-aws-us-west-2 | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.08 | 121425 | 129075 | 129912 | 10034.8 / 14243.2 | 10677.8 / 16278.5 | 0 | 0 | 1 | 21 | 24.20→52.51 |
| chain | resonate | tidb-local | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 0.91 | 13675 | 14719 | 14804 | 222.5 / 906.6 | 541.0 / 3417.4 | 0 | 0 | 4 | 24 | 32.04→28.38 |
| chain | resonate | tidb-local | concurrency=10 l=5 n=20 sleep_ms=1000 wake="push" | 20/20 | 1.15 | 8590 | 9653 | 9703 | 68.7 / 586.7 | 461.0 / 1444.4 | 0 | 0 | 5 | 26 | 17.97→15.88 |
| chain | resonate | tikv | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 0.87 | 15384 | 42199 | 44323 | 38.5 / 949.9 | 1846.1 / 22862.6 | 0 | 0 | 29 | 556 | 23.15→19.39 |
| chain | resonate | tikv | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 1.56 | 11065 | 20420 | 20520 | 30.3 / 834.1 | 567.3 / 9542.6 | 0 | 0 | 44 | 2858 | 19.26→14.28 |
| chain | resonate | tikv | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 1.07 | 17283 | 26767 | 32852 | 59.4 / 722.8 | 2003.3 / 12850.5 | 0 | 0 | 33 | 2749 | 26.33→39.79 |
| chain | resonate | tikv | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 1.16 | 14535 | 30739 | 31175 | 56.2 / 814.9 | 1588.3 / 14553.3 | 0 | 0 | 35 | 1246 | 29.92→28.51 |
| chain | resonate | tikv | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 1.17 | 15835 | 26508 | 28476 | 51.4 / 685.9 | 1562.3 / 7808.8 | 0 | 0 | 34 | 471 | 37.39→29.30 |
| chain | resonate | tikv | concurrency=20 l=5 n=100 sleep_ms=1000 wake="push" | 100/100 | 0.80 | 19363 | 47161 | 55753 | 67.1 / 1003.1 | 2541.1 / 16271.1 | 0 | 0 | 25 | 2749 | 35.08→146.61 |
| chain | temporal | postgres | concurrency=20 l=5 n=100 sleep_ms=1000 | 100/100 | 2.28 | 7259 | 9900 | 10307 | 40.9 / 508.9 | 103.0 / 1845.8 | 0 | 0 | 51 | 601 | 31.36→21.62 |
| chain | temporal | postgres | concurrency=20 l=5 n=100 sleep_ms=1000 | 100/100 | 2.10 | 6452 | 17250 | 17539 | 16.1 / 517.4 | 41.5 / 4636.8 | 0 | 0 | 56 | 591 | 21.79→22.29 |
| chain | temporal | postgres | concurrency=20 l=5 n=100 sleep_ms=1000 | 100/100 | 1.94 | 8806 | 13826 | 17983 | 67.9 / 625.3 | 260.2 / 1659.6 | 0 | 0 | 46 | 586 | 199.68→118.86 |
| chain | temporal-gateway | resonate-postgres | concurrency=20 l=5 n=100 sleep_ms=1000 | 0/0 | 0.00 | 0 | 0 | 0 | 0.0 / 0.0 | – | 0 | 0 | 177 | 299 | 14.60→13.47 |
| fanout | airflow | postgres | concurrency=20 k=8 n=500 poll_ms=100 | 500/500 | 0.68 | 28090 | 37003 | 50484 | 6680.1 / 17302.1 | – | 0 | 0 | 720 | 6434 | 11.92→38.10 |
| fanout | airflow | postgres | concurrency=20 k=8 n=500 poll_ms=100 | 497/500 | 0.42 | 25309 | 45653 | 93020 | 6119.8 / 28651.6 | – | 2171 | 0 | 471 | 6396 | 16.76→22.06 |
| fanout | airflow | postgres | concurrency=20 k=8 n=500 poll_ms=1000 | 496/500 | 0.27 | 39931 | 63041 | 146046 | 9705.0 / 40490.7 | – | 451 | 0 | 393 | 7149 | 31.55→37.64 |
| fanout | loams-durable | tikv | concurrency=20 k=8 n=500 wake="push" | 500/500 | 33.83 | 341 | 1388 | 3530 | 170.6 / 946.6 | – | 0 | 0 | 81 | 472 | 11.52→13.45 |
| fanout | loams-durable | tikv | concurrency=20 k=8 n=500 wake="push" | 500/500 | 62.37 | 244 | 763 | 1093 | 124.9 / 612.0 | – | 0 | 0 | 138 | 2075 | 24.06→24.28 |
| fanout | loams-durable | tikv | concurrency=20 k=8 n=500 wake="push" | 500/500 | 44.76 | 283 | 1624 | 2904 | 138.5 / 2149.4 | – | 0 | 0 | 109 | 2334 | 227.10→179.75 |
| fanout | resonate | postgres | concurrency=20 k=8 n=500 wake="push" | 500/500 | 9.76 | 1994 | 4648 | 6228 | 878.6 / 3561.8 | – | 0 | 0 | 63 | 403 | 23.10→26.82 |
| fanout | resonate | postgres | concurrency=20 k=8 n=500 wake="push" | 500/500 | 18.96 | 669 | 2303 | 5115 | 328.2 / 3468.3 | – | 0 | 0 | 132 | 418 | 29.46→27.97 |
| fanout | resonate | postgres | concurrency=20 k=8 n=500 wake="push" | 500/500 | 17.86 | 813 | 2990 | 4692 | 365.0 / 3176.3 | – | 0 | 0 | 112 | 409 | 25.99→30.67 |
| fanout | resonate | sqlite | concurrency=20 k=8 n=500 wake="push" | 500/500 | 69.84 | 224 | 809 | 952 | 94.9 / 866.2 | – | 0 | 0 | 116 | 98 | 13.29→14.10 |
| fanout | resonate | sqlite | concurrency=20 k=8 n=500 wake="push" | 500/500 | 48.99 | 280 | 1128 | 1843 | 119.2 / 1554.5 | – | 0 | 0 | 104 | 107 | 17.50→18.29 |
| fanout | resonate | sqlite | concurrency=20 k=8 n=500 wake="push" | 500/500 | 15.12 | 889 | 3174 | 10577 | 129.2 / 8872.5 | – | 0 | 0 | 22 | 119 | 52.83→41.66 |
| fanout | resonate | tidb-alicloud-ap-southeast-1 | concurrency=4 k=8 n=40 wake="push" | 39/40 | 0.09 | 41170 | 62382 | 66688 | 25811.2 / 53666.0 | – | 2 | 0 | 0 | 29 | 0.70→0.52 |
| fanout | resonate | tidb-alicloud-ap-southeast-1 | concurrency=4 k=8 n=40 wake="push" | 35/40 | 0.05 | 49023 | 97697 | 138865 | 31603.6 / 128628.3 | – | 40 | 27 | 0 | 22 | 19.87→28.04 |
| fanout | resonate | tidb-aws-ap-northeast-1 | concurrency=4 k=8 n=40 wake="push" | 31/40 | 0.03 | 80966 | 223643 | 237038 | 49765.8 / 205833.8 | – | 128 | 91 | 0 | 20 | 14.10→29.14 |
| fanout | resonate | tidb-aws-ap-northeast-1 | concurrency=4 k=8 n=40 wake="push" | 37/40 | 0.06 | 50273 | 90539 | 96101 | 29697.6 / 77824.8 | – | 8 | 1 | 0 | 21 | 33.14→18.20 |
| fanout | resonate | tidb-aws-ap-southeast-1 | concurrency=4 k=8 n=40 wake="push" | 33/40 | 0.05 | 50938 | 82328 | 89082 | 30723.4 / 145837.1 | – | 47 | 26 | 0 | 18 | 16.41→10.71 |
| fanout | resonate | tidb-aws-ap-southeast-1 | concurrency=4 k=8 n=40 wake="push" | 40/40 | 0.10 | 34810 | 77718 | 80606 | 21675.0 / 68782.8 | – | 0 | 0 | 1 | 21 | 30.81→24.17 |
| fanout | resonate | tidb-aws-eu-central-1 | concurrency=4 k=8 n=40 wake="push" | 29/40 | 0.03 | 81091 | 147741 | 152408 | 53878.8 / 128188.5 | – | 62 | 34 | 0 | 26 | 38.06→14.04 |
| fanout | resonate | tidb-aws-eu-central-1 | concurrency=4 k=8 n=40 wake="push" | 34/40 | 0.04 | 83337 | 134109 | 144347 | 47374.1 / 119857.8 | – | 26 | 13 | 0 | 21 | 20.30→26.25 |
| fanout | resonate | tidb-aws-sa-east-1 | concurrency=4 k=8 n=40 wake="push" | 29/40 | 0.03 | 87671 | 141703 | 147394 | 52023.1 / 122891.6 | – | 37 | 14 | 0 | 30 | 1.53→0.55 |
| fanout | resonate | tidb-aws-sa-east-1 | concurrency=4 k=8 n=40 wake="push" | 31/40 | 0.04 | 84093 | 134241 | 134877 | 50790.1 / 109572.0 | – | 32 | 10 | 0 | 24 | 33.44→28.65 |
| fanout | resonate | tidb-aws-us-east-1 | concurrency=4 k=8 n=40 wake="push" | 32/40 | 0.04 | 82930 | 94958 | 137293 | 51332.9 / 114056.8 | – | 30 | 12 | 0 | 17 | 9.74→1.54 |
| fanout | resonate | tidb-aws-us-east-1 | concurrency=4 k=8 n=40 wake="push" | 30/40 | 0.02 | 141260 | 177667 | 180058 | 71450.1 / 161120.3 | – | 171 | 122 | 0 | 22 | 23.76→92.66 |
| fanout | resonate | tidb-aws-us-west-2 | concurrency=4 k=8 n=40 wake="push" | 29/40 | 0.03 | 79456 | 149893 | 154178 | 48952.0 / 125532.4 | – | 42 | 16 | 0 | 31 | 1.92→0.99 |
| fanout | resonate | tidb-aws-us-west-2 | concurrency=4 k=8 n=40 wake="push" | 33/40 | 0.04 | 64357 | 83057 | 168286 | 41938.0 / 137037.1 | – | 39 | 19 | 0 | 27 | 23.68→24.20 |
| fanout | resonate | tidb-local | concurrency=10 k=8 n=100 wake="push" | 100/100 | 3.41 | 2987 | 4483 | 4616 | 1752.5 / 3582.5 | – | 0 | 0 | 8 | 30 | 37.80→34.40 |
| fanout | resonate | tidb-local | concurrency=10 k=8 n=100 wake="push" | 100/100 | 5.39 | 1148 | 5183 | 5910 | 630.9 / 4030.6 | – | 0 | 0 | 11 | 34 | 17.08→17.97 |
| fanout | resonate | tikv | concurrency=20 k=8 n=500 wake="push" | 497/500 | 3.70 | 162 | 938 | 116763 | 75.0 / 652.8 | – | 36 | 30 | 35 | 2312 | 17.51→23.15 |
| fanout | resonate | tikv | concurrency=20 k=8 n=500 wake="push" | 500/500 | 120.99 | 147 | 222 | 278 | 74.0 / 150.7 | – | 0 | 0 | 256 | 2859 | 19.72→19.26 |
| fanout | resonate | tikv | concurrency=20 k=8 n=500 wake="push" | 500/500 | 37.48 | 279 | 1438 | 2582 | 136.2 / 1200.1 | – | 0 | 0 | 92 | 2763 | 26.27→26.33 |
| fanout | resonate | tikv | concurrency=20 k=8 n=500 wake="push" | 500/500 | 48.39 | 290 | 789 | 2054 | 140.0 / 1820.4 | – | 0 | 0 | 129 | 1966 | 28.77→29.92 |
| fanout | resonate | tikv | concurrency=20 k=8 n=500 wake="push" | 500/500 | 30.62 | 329 | 1871 | 2311 | 156.1 / 1352.1 | – | 0 | 0 | 91 | 2009 | 41.74→37.39 |
| fanout | resonate | tikv | concurrency=20 k=8 n=500 wake="push" | 500/500 | 49.44 | 291 | 821 | 1414 | 133.9 / 1136.7 | – | 0 | 0 | 121 | 2771 | 39.78→36.22 |
| fanout | temporal | postgres | concurrency=20 k=8 n=500 | 500/500 | 3.32 | 4163 | 18444 | 26968 | 460.8 / 11633.9 | – | 0 | 6 | 52 | 557 | 15.66→31.36 |
| fanout | temporal | postgres | concurrency=20 k=8 n=500 | 500/500 | 9.09 | 1555 | 6050 | 8096 | 287.9 / 4773.8 | – | 0 | 0 | 141 | 593 | 18.39→21.79 |
| fanout | temporal | postgres | concurrency=20 k=8 n=500 | 500/500 | 9.54 | 1878 | 4201 | 5432 | 290.9 / 3433.4 | – | 0 | 1 | 132 | 587 | 232.05→199.68 |
| fanout | temporal | postgres | concurrency=20 k=8 n=500 | 498/500 | 7.00 | 583 | 8729 | 35276 | 144.7 / 28835.5 | – | 2 | 15 | 111 | 667 | 9.89→27.99 |
| fanout | temporal-gateway | resonate-postgres | concurrency=20 k=8 n=500 | 500/500 | 1.31 | 15730 | 24719 | 28159 | 3461.6 / 14431.0 | – | 0 | 0 | 149 | 331 | 25.73→15.00 |
| fanout | temporal-gateway | resonate-postgres | concurrency=20 k=8 n=500 | 220/220 | 0.95 | 21731 | 40936 | 42064 | 4418.7 / 24512.0 | – | 0 | 0 | 65 | 375 | 39.45→24.38 |
