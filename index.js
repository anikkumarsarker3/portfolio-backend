const express = require('express');
const cors = require('cors');
const http = require('http');
const socketIo = require('socket.io');
const { MongoClient, ServerApiVersion, ObjectId } = require('mongodb');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST", "PUT", "DELETE", "PATCH"]
    }
});

// Middleware
app.use(cors());
app.use(express.json());

const port = process.env.PORT || 3000;
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || 'dev-token-123';
const isValidObjectId = (id) => ObjectId.isValid(id) && String(new ObjectId(id)) === id;
const normalizeEmail = (email) => {
    if (!email || typeof email !== 'string') return '';
    return email.trim().toLowerCase();
};
const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const normalizeObjectId = (value) => {
    if (!value) return null;
    if (value instanceof ObjectId) return value;
    if (typeof value?.toHexString === 'function') {
        const hex = value.toHexString();
        if (typeof hex === 'string' && isValidObjectId(hex)) {
            return new ObjectId(hex);
        }
    }

    if (typeof value === 'string') {
        const trimmed = value.trim();
        if (isValidObjectId(trimmed)) {
            return new ObjectId(trimmed);
        }
        return null;
    }

    if (typeof value === 'object') {
        const candidate = value.$oid || value._id || value.id;
        if (typeof candidate === 'string' && isValidObjectId(candidate.trim())) {
            return new ObjectId(candidate.trim());
        }
    }

    return null;
};

// MongoDB connection
const uri = process.env.MONGODB_URI;
const dbName = process.env.DB_NAME || 'anikPortfolio';

if (!uri) {
    throw new Error('Missing MONGODB_URI in environment variables');
}
const client = new MongoClient(uri, {
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    }
});

// Admin authentication middleware
const authenticateAdmin = (req, res, next) => {
    const token = req.headers['admin-token'];
    if (!token || token !== ADMIN_TOKEN) {
        return res.status(401).json({
            status: false,
            message: 'Unauthorized: Invalid admin token'
        });
    }
    next();
};

async function run() {
    try {
        // Connect to MongoDB
        await client.connect();
        console.log("Successfully connected to MongoDB!");

        const db = client.db(dbName);
        const projectCollection = db.collection("projects");
        const experienceCollection = db.collection("experiences");
        const achievementCollection = db.collection("achievements");
        const conversationCollection = db.collection("conversations");

        // Socket.IO connection handling
        io.on('connection', (socket) => {
            console.log('New client connected:', socket.id);

            socket.on('join_conversation', async (conversationId) => {
                try {
                    if (!conversationId) return;
                    const conversationObjectId = normalizeObjectId(conversationId);
                    if (!conversationObjectId) {
                        socket.emit('error', { message: 'Invalid conversation ID' });
                        return;
                    }
                    const normalizedConversationId = conversationObjectId.toHexString();
                    socket.join(normalizedConversationId);

                    const conversation = await conversationCollection.findOne({
                        _id: conversationObjectId
                    });

                    if (!conversation) {
                        socket.emit('error', { message: 'Conversation not found' });
                        return;
                    }

                    socket.emit('conversation_history', conversation.messages || []);
                } catch (error) {
                    socket.emit('error', { message: 'Failed to join conversation' });
                }
            });

            socket.on('send_message', async (payload, ack) => {
                const safeAck = typeof ack === 'function' ? ack : () => {};
                try {
                    const { conversationId, message, sender = 'visitor', clientMessageId } = payload || {};
                    if (!conversationId || !message) {
                        socket.emit('error', { message: 'Conversation ID and message are required' });
                        safeAck({ status: false, message: 'Conversation ID and message are required' });
                        return;
                    }
                    const conversationObjectId = normalizeObjectId(conversationId);
                    if (!conversationObjectId) {
                        socket.emit('error', { message: 'Invalid conversation ID' });
                        safeAck({ status: false, message: 'Invalid conversation ID' });
                        return;
                    }
                    const normalizedConversationId = conversationObjectId.toHexString();

                    const timestamp = new Date();
                    const newMessage = {
                        _id: new ObjectId(),
                        message,
                        sender,
                        timestamp,
                        clientMessageId: clientMessageId || null,
                        isRead: false
                    };

                    const updateOps = {
                        $push: { messages: newMessage },
                        $set: { updatedAt: timestamp }
                    };

                    if (sender === 'visitor' || sender === 'user') {
                        updateOps.$inc = { unreadCount: 1 };
                    }

                    const result = await conversationCollection.updateOne(
                        { _id: conversationObjectId },
                        updateOps
                    );

                    if (result.matchedCount === 0) {
                        socket.emit('error', { message: 'Conversation not found' });
                        safeAck({ status: false, message: 'Conversation not found' });
                        return;
                    }

                    const messagePayload = {
                        ...newMessage,
                        conversationId: normalizedConversationId
                    };

                    // Always confirm to sender, even if room join did not complete yet.
                    socket.emit('message_received', messagePayload);
                    // Broadcast to other participants in the same conversation room.
                    socket.to(normalizedConversationId).emit('message_received', messagePayload);
                    io.emit('new_message', { conversationId: normalizedConversationId, message: newMessage });
                    io.emit('conversation_updated', { conversationId: normalizedConversationId, updatedAt: timestamp });
                    safeAck({
                        status: true,
                        conversationId: normalizedConversationId,
                        messageId: newMessage._id.toHexString()
                    });
                } catch (error) {
                    socket.emit('error', { message: 'Failed to send message' });
                    safeAck({ status: false, message: 'Failed to send message' });
                }
            });

            socket.on('typing', (data) => {
                if (!data?.conversationId) return;
                socket.to(data.conversationId).emit('user_typing', data);
            });

            socket.on('disconnect', () => {
                console.log('Client disconnected:', socket.id);
            });
        });

        // ==================== ADMIN STATS ====================
        app.get("/api/admin/stats", authenticateAdmin, async (req, res) => {
            try {
                const totalProjects = await projectCollection.countDocuments();
                const totalExperience = await experienceCollection.countDocuments();
                const totalAchievements = await achievementCollection.countDocuments();

                // Count total messages
                const conversations = await conversationCollection.find().toArray();
                const totalMessages = conversations.reduce((sum, conv) =>
                    sum + (conv.messages?.length || 0), 0
                );

                // Count unread messages
                const unreadMessages = conversations.reduce((sum, conv) =>
                    sum + (conv.unreadCount || 0), 0
                );

                res.json({
                    totalMessages,
                    unreadMessages,
                    totalProjects,
                    totalExperience,
                    totalAchievements
                });
            } catch (error) {
                console.error('Error fetching stats:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to fetch stats'
                });
            }
        });

        // ==================== CONVERSATIONS ====================

        // Get all conversations
        app.get("/api/admin/conversations", authenticateAdmin, async (req, res) => {
            try {
                const conversations = await conversationCollection
                    .find()
                    .sort({ updatedAt: -1 })
                    .toArray();

                // Add message count to each conversation
                const conversationsWithCount = conversations.map(conv => ({
                    ...conv,
                    messageCount: conv.messages?.length || 0
                }));

                res.json(conversationsWithCount);
            } catch (error) {
                console.error('Error fetching conversations:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to fetch conversations'
                });
            }
        });

        // Get single conversation
        app.get("/api/admin/conversations/:id", authenticateAdmin, async (req, res) => {
            try {
                const { id } = req.params;
                if (!isValidObjectId(id)) {
                    return res.status(400).json({
                        status: false,
                        message: 'Invalid conversation ID'
                    });
                }
                const conversation = await conversationCollection.findOne({
                    _id: new ObjectId(id)
                });

                if (!conversation) {
                    return res.status(404).json({
                        status: false,
                        message: 'Conversation not found'
                    });
                }

                res.json(conversation);
            } catch (error) {
                console.error('Error fetching conversation:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to fetch conversation'
                });
            }
        });

        // Mark conversation as read
        app.put("/api/admin/conversations/:id/mark-read", authenticateAdmin, async (req, res) => {
            try {
                const { id } = req.params;
                if (!isValidObjectId(id)) {
                    return res.status(400).json({
                        status: false,
                        message: 'Invalid conversation ID'
                    });
                }

                const result = await conversationCollection.updateOne(
                    { _id: new ObjectId(id) },
                    {
                        $set: {
                            unreadCount: 0,
                            'messages.$[elem].isRead': true
                        }
                    },
                    {
                        arrayFilters: [{ 'elem.sender': { $in: ['visitor', 'user'] } }]
                    }
                );

                res.json({
                    status: true,
                    message: 'Conversation marked as read',
                    result
                });
            } catch (error) {
                console.error('Error marking conversation as read:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to mark conversation as read'
                });
            }
        });

        // Send reply to conversation
        app.post("/api/admin/send-reply", authenticateAdmin, async (req, res) => {
            try {
                const { conversationId, message } = req.body;

                if (!conversationId || !message) {
                    return res.status(400).json({
                        status: false,
                        message: 'Conversation ID and message are required'
                    });
                }
                const conversationObjectId = normalizeObjectId(conversationId);
                if (!conversationObjectId) {
                    return res.status(400).json({
                        status: false,
                        message: 'Invalid conversation ID'
                    });
                }
                const normalizedConversationId = conversationObjectId.toHexString();

                const newMessage = {
                    _id: new ObjectId(),
                    message,
                    sender: 'admin',
                    timestamp: new Date(),
                    isRead: false
                };

                const result = await conversationCollection.updateOne(
                    { _id: conversationObjectId },
                    {
                        $push: { messages: newMessage },
                        $set: { updatedAt: new Date() }
                    }
                );

                if (result.modifiedCount === 0) {
                    return res.status(404).json({
                        status: false,
                        message: 'Conversation not found'
                    });
                }

                // Emit socket event for real-time update
                io.to(normalizedConversationId).emit('admin_reply', {
                    conversationId: normalizedConversationId,
                    message: newMessage
                });
                io.to(normalizedConversationId).emit('message_received', {
                    ...newMessage,
                    conversationId: normalizedConversationId
                });

                io.emit('new_message', {
                    conversationId: normalizedConversationId,
                    message: newMessage
                });

                io.emit('conversation_updated', {
                    conversationId: normalizedConversationId,
                    updatedAt: new Date()
                });

                res.json({
                    status: true,
                    message: 'Reply sent successfully',
                    data: newMessage
                });
            } catch (error) {
                console.error('Error sending reply:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to send reply'
                });
            }
        });

        // Create new conversation (from contact form)
        app.post("/api/contact", async (req, res) => {
            try {
                const { name, email, message } = req.body;
                const normalizedName = typeof name === 'string' ? name.trim() : '';
                const normalizedEmailRaw = typeof email === 'string' ? email.trim() : '';
                const normalizedEmail = normalizeEmail(email);
                const normalizedMessage = typeof message === 'string' ? message.trim() : '';

                if (!normalizedName || !normalizedEmail || !normalizedMessage) {
                    return res.status(400).json({
                        status: false,
                        message: 'Name, email, and message are required'
                    });
                }

                const newMessage = {
                    _id: new ObjectId(),
                    message: normalizedMessage,
                    sender: 'visitor',
                    timestamp: new Date(),
                    isRead: false
                };

                const existingConversation = await conversationCollection.findOne(
                    {
                        $or: [
                            { emailLower: normalizedEmail },
                            { email: { $regex: `^${escapeRegExp(normalizedEmailRaw)}$`, $options: 'i' } }
                        ]
                    },
                    { sort: { updatedAt: -1 } }
                );

                let conversationId;
                if (existingConversation?._id) {
                    await conversationCollection.updateOne(
                        { _id: existingConversation._id },
                        {
                            $push: { messages: newMessage },
                            $inc: { unreadCount: 1 },
                            $set: {
                                name: normalizedName,
                                email: normalizedEmailRaw,
                                emailLower: normalizedEmail,
                                updatedAt: new Date()
                            }
                        }
                    );
                    conversationId = existingConversation._id.toHexString();
                } else {
                    const conversation = {
                        name: normalizedName,
                        email: normalizedEmailRaw,
                        emailLower: normalizedEmail,
                        messages: [newMessage],
                        unreadCount: 1,
                        createdAt: new Date(),
                        updatedAt: new Date()
                    };

                    const result = await conversationCollection.insertOne(conversation);
                    conversationId = result.insertedId.toHexString();
                }

                // Emit socket event
                io.emit('new_message', {
                    conversationId,
                    message: newMessage
                });
                io.emit('conversation_updated', {
                    conversationId,
                    updatedAt: new Date()
                });

                res.json({
                    status: true,
                    message: 'Message sent successfully',
                    conversationId
                });
            } catch (error) {
                console.error('Error creating conversation:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to send message'
                });
            }
        });

        // Start conversation (for floating message button)
        app.post("/api/start-conversation", async (req, res) => {
            try {
                const { name, email } = req.body;
                const normalizedName = typeof name === 'string' ? name.trim() : '';
                const normalizedEmailRaw = typeof email === 'string' ? email.trim() : '';
                const normalizedEmail = normalizeEmail(email);
                if (!normalizedName || !normalizedEmail) {
                    return res.status(400).json({
                        status: false,
                        error: 'Name and email are required'
                    });
                }
                const existingConversation = await conversationCollection.findOne(
                    {
                        $or: [
                            { emailLower: normalizedEmail },
                            { email: { $regex: `^${escapeRegExp(normalizedEmailRaw)}$`, $options: 'i' } }
                        ]
                    },
                    { sort: { updatedAt: -1 } }
                );

                if (existingConversation?._id) {
                    await conversationCollection.updateOne(
                        { _id: existingConversation._id },
                        {
                            $set: {
                                name: normalizedName,
                                email: normalizedEmailRaw,
                                emailLower: normalizedEmail,
                                updatedAt: new Date()
                            }
                        }
                    );

                    return res.json({
                        status: true,
                        conversationId: existingConversation._id.toHexString()
                    });
                }

                const conversation = {
                    name: normalizedName,
                    email: normalizedEmailRaw,
                    emailLower: normalizedEmail,
                    messages: [],
                    unreadCount: 0,
                    createdAt: new Date(),
                    updatedAt: new Date()
                };

                const result = await conversationCollection.insertOne(conversation);
                res.json({
                    status: true,
                    conversationId: result.insertedId.toHexString()
                });
            } catch (error) {
                console.error('Error starting conversation:', error);
                res.status(500).json({
                    status: false,
                    error: 'Failed to start conversation'
                });
            }
        });

        // Get conversation messages for visitor chat
        app.get("/api/conversations/:id/messages", async (req, res) => {
            try {
                const { id } = req.params;
                if (!isValidObjectId(id)) {
                    return res.status(400).json({
                        status: false,
                        message: 'Invalid conversation ID'
                    });
                }
                const conversation = await conversationCollection.findOne(
                    { _id: new ObjectId(id) },
                    { projection: { messages: 1 } }
                );

                if (!conversation) {
                    return res.status(404).json({
                        status: false,
                        message: 'Conversation not found'
                    });
                }

                res.json(conversation.messages || []);
            } catch (error) {
                console.error('Error fetching conversation messages:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to fetch conversation messages'
                });
            }
        });

        // ==================== PROJECTS ====================

        // Get all projects
        app.get("/api/admin/projects", authenticateAdmin, async (req, res) => {
            try {
                const projects = await projectCollection
                    .find()
                    .sort({ date: -1 })
                    .toArray();
                res.json(projects);
            } catch (error) {
                console.error('Error fetching projects:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to fetch projects'
                });
            }
        });

        // Get public projects (no auth required)
        app.get("/api/projects", async (req, res) => {
            try {
                const projects = await projectCollection
                    .find()
                    .sort({ date: -1 })
                    .toArray();
                res.json({
                    status: true,
                    result: projects
                });
            } catch (error) {
                console.error('Error fetching projects:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to fetch projects'
                });
            }
        });

        // Create project
        app.post("/api/admin/projects", authenticateAdmin, async (req, res) => {
            try {
                const project = req.body;

                // Add timestamps
                project.createdAt = new Date();
                project.updatedAt = new Date();

                const result = await projectCollection.insertOne(project);
                res.json({
                    status: true,
                    message: 'Project created successfully',
                    projectId: result.insertedId
                });
            } catch (error) {
                console.error('Error creating project:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to create project'
                });
            }
        });

        // Update project
        app.put("/api/admin/projects/:id", authenticateAdmin, async (req, res) => {
            try {
                const { id } = req.params;
                const updates = req.body;

                // Update timestamp
                updates.updatedAt = new Date();

                const result = await projectCollection.updateOne(
                    { _id: new ObjectId(id) },
                    { $set: updates }
                );

                if (result.matchedCount === 0) {
                    return res.status(404).json({
                        status: false,
                        message: 'Project not found'
                    });
                }

                res.json({
                    status: true,
                    message: 'Project updated successfully'
                });
            } catch (error) {
                console.error('Error updating project:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to update project'
                });
            }
        });

        // Delete project
        app.delete("/api/admin/projects/:id", authenticateAdmin, async (req, res) => {
            try {
                const { id } = req.params;
                const result = await projectCollection.deleteOne({
                    _id: new ObjectId(id)
                });

                if (result.deletedCount === 0) {
                    return res.status(404).json({
                        status: false,
                        message: 'Project not found'
                    });
                }

                res.json({
                    status: true,
                    message: 'Project deleted successfully'
                });
            } catch (error) {
                console.error('Error deleting project:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to delete project'
                });
            }
        });

        // ==================== EXPERIENCES ====================

        // Get all experiences
        app.get("/api/admin/experiences", authenticateAdmin, async (req, res) => {
            try {
                const experiences = await experienceCollection
                    .find()
                    .sort({ period: -1 })
                    .toArray();
                res.json(experiences);
            } catch (error) {
                console.error('Error fetching experiences:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to fetch experiences'
                });
            }
        });

        // Get public experiences (no auth required)
        app.get("/api/experiences", async (req, res) => {
            try {
                const experiences = await experienceCollection
                    .find()
                    .sort({ period: -1 })
                    .toArray();
                res.json({
                    status: true,
                    result: experiences
                });
            } catch (error) {
                console.error('Error fetching experiences:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to fetch experiences'
                });
            }
        });

        // Create experience
        app.post("/api/admin/experiences", authenticateAdmin, async (req, res) => {
            try {
                const experience = req.body;

                // Add timestamps
                experience.createdAt = new Date();
                experience.updatedAt = new Date();

                const result = await experienceCollection.insertOne(experience);
                res.json({
                    status: true,
                    message: 'Experience created successfully',
                    experienceId: result.insertedId
                });
            } catch (error) {
                console.error('Error creating experience:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to create experience'
                });
            }
        });

        // Update experience
        app.put("/api/admin/experiences/:id", authenticateAdmin, async (req, res) => {
            try {
                const { id } = req.params;
                const updates = req.body;

                // Update timestamp
                updates.updatedAt = new Date();

                const result = await experienceCollection.updateOne(
                    { _id: new ObjectId(id) },
                    { $set: updates }
                );

                if (result.matchedCount === 0) {
                    return res.status(404).json({
                        status: false,
                        message: 'Experience not found'
                    });
                }

                res.json({
                    status: true,
                    message: 'Experience updated successfully'
                });
            } catch (error) {
                console.error('Error updating experience:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to update experience'
                });
            }
        });

        // Delete experience
        app.delete("/api/admin/experiences/:id", authenticateAdmin, async (req, res) => {
            try {
                const { id } = req.params;
                const result = await experienceCollection.deleteOne({
                    _id: new ObjectId(id)
                });

                if (result.deletedCount === 0) {
                    return res.status(404).json({
                        status: false,
                        message: 'Experience not found'
                    });
                }

                res.json({
                    status: true,
                    message: 'Experience deleted successfully'
                });
            } catch (error) {
                console.error('Error deleting experience:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to delete experience'
                });
            }
        });

        // ==================== ACHIEVEMENTS ====================

        app.get("/api/admin/achievements", authenticateAdmin, async (req, res) => {
            try {
                const achievements = await achievementCollection
                    .find()
                    .sort({ year: -1, createdAt: -1 })
                    .toArray();
                res.json(achievements);
            } catch (error) {
                console.error('Error fetching achievements:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to fetch achievements'
                });
            }
        });

        app.get("/api/achievements", async (req, res) => {
            try {
                const achievements = await achievementCollection
                    .find()
                    .sort({ year: -1, createdAt: -1 })
                    .toArray();
                res.json({
                    status: true,
                    result: achievements
                });
            } catch (error) {
                console.error('Error fetching achievements:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to fetch achievements'
                });
            }
        });

        app.post("/api/admin/achievements", authenticateAdmin, async (req, res) => {
            try {
                const achievement = req.body;
                achievement.createdAt = new Date();
                achievement.updatedAt = new Date();

                const result = await achievementCollection.insertOne(achievement);
                res.json({
                    status: true,
                    message: 'Achievement created successfully',
                    achievementId: result.insertedId
                });
            } catch (error) {
                console.error('Error creating achievement:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to create achievement'
                });
            }
        });

        app.put("/api/admin/achievements/:id", authenticateAdmin, async (req, res) => {
            try {
                const { id } = req.params;
                const updates = req.body;
                updates.updatedAt = new Date();

                const result = await achievementCollection.updateOne(
                    { _id: new ObjectId(id) },
                    { $set: updates }
                );

                if (result.matchedCount === 0) {
                    return res.status(404).json({
                        status: false,
                        message: 'Achievement not found'
                    });
                }

                res.json({
                    status: true,
                    message: 'Achievement updated successfully'
                });
            } catch (error) {
                console.error('Error updating achievement:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to update achievement'
                });
            }
        });

        app.delete("/api/admin/achievements/:id", authenticateAdmin, async (req, res) => {
            try {
                const { id } = req.params;
                const result = await achievementCollection.deleteOne({
                    _id: new ObjectId(id)
                });

                if (result.deletedCount === 0) {
                    return res.status(404).json({
                        status: false,
                        message: 'Achievement not found'
                    });
                }

                res.json({
                    status: true,
                    message: 'Achievement deleted successfully'
                });
            } catch (error) {
                console.error('Error deleting achievement:', error);
                res.status(500).json({
                    status: false,
                    message: 'Failed to delete achievement'
                });
            }
        });

        // Ping MongoDB
        await client.db("admin").command({ ping: 1 });
        console.log("Pinged your deployment. You successfully connected to MongoDB!");

    } catch (error) {
        console.error('Error in server setup:', error);
        throw error;
    }
}

// Root route
app.get('/', (req, res) => {
    res.json({
        status: true,
        message: 'Anik Portfolio Server Running....',
        version: '2.0.0',
        endpoints: {
            admin: {
                stats: 'GET /api/admin/stats',
                conversations: 'GET /api/admin/conversations',
                conversation: 'GET /api/admin/conversations/:id',
                markRead: 'PUT /api/admin/conversations/:id/mark-read',
                sendReply: 'POST /api/admin/send-reply',
                projects: {
                    list: 'GET /api/admin/projects',
                    create: 'POST /api/admin/projects',
                    update: 'PUT /api/admin/projects/:id',
                    delete: 'DELETE /api/admin/projects/:id'
                },
                experiences: {
                    list: 'GET /api/admin/experiences',
                    create: 'POST /api/admin/experiences',
                    update: 'PUT /api/admin/experiences/:id',
                    delete: 'DELETE /api/admin/experiences/:id'
                },
                achievements: {
                    list: 'GET /api/admin/achievements',
                    create: 'POST /api/admin/achievements',
                    update: 'PUT /api/admin/achievements/:id',
                    delete: 'DELETE /api/admin/achievements/:id'
                }
            },
            public: {
                contact: 'POST /api/contact',
                startConversation: 'POST /api/start-conversation',
                conversationMessages: 'GET /api/conversations/:id/messages',
                projects: 'GET /api/projects',
                experiences: 'GET /api/experiences',
                achievements: 'GET /api/achievements'
            }
        }
    });
});

const registerTerminalMiddleware = () => {
    // Error handler must be registered after all routes.
    app.use((err, req, res, next) => {
        console.error('Server Error:', err);
        res.status(500).json({
            status: false,
            message: 'Internal server error',
            error: process.env.NODE_ENV === 'development' ? err.message : undefined
        });
    });

    // 404 should be last.
    app.use((req, res) => {
        res.status(404).json({
            status: false,
            message: 'Route not found'
        });
    });
};

// Graceful shutdown
process.on('SIGTERM', async () => {
    console.log('SIGTERM signal received: closing HTTP server');
    server.close(async () => {
        console.log('HTTP server closed');
        await client.close();
        console.log('MongoDB connection closed');
        process.exit(0);
    });
});

run()
    .then(() => {
        registerTerminalMiddleware();
        server.listen(port, () => {
            console.log(`Server running on port ${port}`);
            console.log('Socket.IO enabled for real-time updates');
            console.log('Admin token authentication active');
        });
    })
    .catch((error) => {
        console.error('Failed to initialize server:', error.message);
        process.exit(1);
    });

