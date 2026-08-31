FROM python:3.12-alpine
WORKDIR /app
COPY raftstore ./raftstore
RUN addgroup -S raft && adduser -S raft -G raft && mkdir /data && chown raft:raft /data
USER raft
EXPOSE 8000
ENTRYPOINT ["python", "-m", "raftstore.server"]

