FROM node:22-bookworm-slim
WORKDIR /app/sensor_typescript
COPY ./sensor_typescript ./
RUN node --experimental-strip-types --test tests/*.test.ts
USER node
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD node -e "try { process.kill(1, 0); } catch { process.exit(1); }"
CMD ["node", "--experimental-strip-types", "src/main.ts"]
