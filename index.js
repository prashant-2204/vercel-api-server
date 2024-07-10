const express = require("express");
const { generateSlug } = require("random-word-slugs");
const { ECSClient, RunTaskCommand } = require("@aws-sdk/client-ecs");
const Redis = require('ioredis');
const { Server } = require("socket.io");
const cors = require("cors");
const dotenv = require("dotenv");

dotenv.config();

const app = express();
const PORT = process.env.PORT || 9000;

const subscriber = new Redis(process.env.REDIS_URL);

const io = new Server({ cors: '*' });
app.use(cors());

io.on('connection', (socket) => {
  socket.on('subscribe', (channel) => {
    socket.join(channel);
    socket.emit('message', `Joined ${channel}`);
  });
});

io.listen(process.env.SOCKET_PORT || 9002, () => {
  console.log(`Socket server running on port ${process.env.SOCKET_PORT || 9002}`);
});

const ecsClient = new ECSClient({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

app.use(express.json());

app.post("/project", async (req, res) => {
  const { gitURL } = req.body;
  const projectSlug = generateSlug();
  const config = {
    CLUSTER: process.env.ECS_CLUSTER,
    TASK: process.env.ECS_TASK_DEFINITION,
  };
  const command = new RunTaskCommand({
    cluster: config.CLUSTER,
    taskDefinition: config.TASK,
    launchType: "FARGATE",
    count: 1,
    networkConfiguration: {
      awsvpcConfiguration: {
        subnets: process.env.SUBNETS.split(','),
        assignPublicIp: "ENABLED",
        securityGroups: [process.env.SECURITY_GROUP],
      },
    },
    overrides: {
      containerOverrides: [
        {
          name: "builder-image",
          environment: [
            {
              name: "GIT_REPOSITORY_URL",
              value: gitURL,
            },
            {
              name: "PROJECT_ID",
              value: projectSlug,
            },
          ],
        },
      ],
    },
  });
  await ecsClient.send(command);
  return res.json({
    status: "queued",
    data: { projectSlug, url: `http://${projectSlug}.vercel-s3-reverse-proxy.onrender.com` }, //https://vercel-s3-reverse-proxy.onrender.com
  });
});

async function initRedisSubscribe() {
  console.log('Subscribed to logs....');
  subscriber.psubscribe('logs:*');
  subscriber.on('pmessage', (pattern, channel, message) => {
    io.to(channel).emit('message', message);
  });
}

initRedisSubscribe();

app.listen(PORT, () => {
  console.log(`API server is running on port ${PORT}`);
});
