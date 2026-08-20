const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, '..', 'web')));

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        uptime: process.uptime(),
        androidConnected,
        dashboardClients: dashboards.size,
        framesRelayed: totalFramesRelayed
    });
});

const server = http.createServer(app);

const io = new Server(server, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    pingTimeout: 60000,
    pingInterval: 25000,
    maxHttpBufferSize: 1e6,
    transports: ['websocket', 'polling']
});

let androidConnected = false;
let androidSocket = null;
const dashboards = new Set();
let totalFramesRelayed = 0;

io.on('connection', (socket) => {
    console.log(`[+] Connected: ${socket.id}`);

    socket.on('register-android', () => {
        androidConnected = true;
        androidSocket = socket;
        socket.data.isAndroid = true;
        console.log(`[Android] Device online: ${socket.id}`);
        for (const id of dashboards) {
            const s = io.sockets.sockets.get(id);
            if (s) s.emit('android-status', { connected: true });
        }
    });

    socket.on('register-dashboard', () => {
        socket.data.isDashboard = true;
        socket.data.isStreaming = false;
        dashboards.add(socket.id);
        console.log(`[Dashboard] Connected: ${socket.id} (total: ${dashboards.size})`);
        socket.emit('android-status', { connected: androidConnected });
    });

    socket.on('start-stream', () => {
        if (!socket.data.isDashboard) return;
        socket.data.isStreaming = true;
        console.log(`[Dashboard] STARTED: ${socket.id}`);
    });

    socket.on('stop-stream', () => {
        if (!socket.data.isDashboard) return;
        socket.data.isStreaming = false;
        console.log(`[Dashboard] STOPPED: ${socket.id}`);
    });

    // Frame from Android → broadcast to ALL streaming dashboards
    // NO backpressure, NO pending counter, NO dropping on server
    // Server is dumb relay only
    socket.on('frame', (base64Data) => {
        if (!socket.data.isAndroid) return;

        totalFramesRelayed++;

        for (const dashId of dashboards) {
            const dashSocket = io.sockets.sockets.get(dashId);
            if (!dashSocket || !dashSocket.connected) continue;
            if (!dashSocket.data.isStreaming) continue;

            // Send frame to dashboard
            dashSocket.emit('frame', base64Data);
        }
    });

    socket.on('rotation', (meta) => {
        if (!socket.data.isAndroid) return;
        console.log(`[Android] Rotation: ${meta.width}x${meta.height}`);
        for (const id of dashboards) {
            const s = io.sockets.sockets.get(id);
            if (s) s.emit('rotation', meta);
        }
    });

    socket.on('disconnect', () => {
        console.log(`[-] Disconnected: ${socket.id}`);
        if (socket.data.isAndroid) {
            androidConnected = false;
            androidSocket = null;
            for (const id of dashboards) {
                const s = io.sockets.sockets.get(id);
                if (s) s.emit('android-status', { connected: false });
            }
            console.log(`[Android] Device offline`);
        }
        if (socket.data.isDashboard) {
            dashboards.delete(socket.id);
            console.log(`[Dashboard] Left (remaining: ${dashboards.size})`);
        }
    });
});

setInterval(() => {
    console.log(
        `[Stats] Android: ${androidConnected ? 'ON' : 'OFF'} | ` +
        `Dashboards: ${dashboards.size} | ` +
        `Frames relayed: ${totalFramesRelayed}`
    );
}, 30000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`========================================`);
    console.log(` Screen Monitor Server`);
    console.log(` Port: ${PORT}`);
    console.log(`========================================`);
});
