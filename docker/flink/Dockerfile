# ==================================================================
# Flink 1.20.1 (Scala 2.12 / Java 17) — Phase 9 最小构建
#
# 目的只有一个：在官方镜像里预建 checkpoint / savepoint 目录并归属 flink
# 用户，使 Docker 命名卷首次挂载时继承正确属主（否则卷目录由 root 创建，
# flink 用户无法写入 checkpoint）。
#
# 不安装任何业务代码；作业仍通过 `flink run` 提交官方 examples 或后续
# 9.2/9.3 的 jar。基础镜像 tag 固定，禁止 :latest。
# 大陆拉取：docker pull docker.1ms.run/flink:1.20.1-scala_2.12-java17
#           docker tag  docker.1ms.run/flink:1.20.1-scala_2.12-java17 \
#                       flink:1.20.1-scala_2.12-java17
# ==================================================================
FROM flink:1.20.1-scala_2.12-java17

USER root
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints \
    && chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints

# Phase 9.3：Kafka connector 与 MySQL JDBC 驱动（版本锁定，由
# scripts/download_flink_jars.sh 下载；jar 本身不提交仓库）。
COPY docker/flink/lib/*.jar /opt/flink/lib/

USER flink
