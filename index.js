const express = require('express');
const cors = require('cors')
const app = express()
require('dotenv').config()
app.use(cors())
app.use(express.json())
const port = process.env.PORT || 3000
const { MongoClient, ServerApiVersion } = require('mongodb');


const uri = process.env.MONGODB_URI;
// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri, {
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    }
});

async function run() {
    try {
        // Connect the client to the server	(optional starting in v4.7)
        await client.connect();
        const db = client.db("anikPortfolio")
        const projectCollection = db.collection("project")
        const experienceCollection = db.collection("experince")

        app.post("/project", async (req, res) => {
            const project = req.body();
            const result = await projectCollection.insertOne(project);
            res.json({
                status: true,
                result
            })
        })
        app.get("/project", async (req, res) => {
            const result = await projectCollection.find().toArray();
            res.json({
                status: true,
                result
            })
        })
        app.patch("/project/:id", async (req, res) => {

        })
        app.delete("/project/:id", async (req, res) => {

        })



        app.post("/experience", async (req, res) => {
            const experience = req.body();
            const result = await experienceCollection.insertOne(experience)
            res.json({
                status: true,
                result
            })

        })
        app.get("/experience", async (req, res) => {
            const result = await experienceCollection.find().toArray();
            res.json({
                status: true,
                result
            })
        })
        app.patch("/experience/:id", async (req, res) => {

        })
        app.delete("/experience/:id", async (req, res) => {

        })



        // Send a ping to confirm a successful connection
        await client.db("admin").command({ ping: 1 });
        console.log("Pinged your deployment. You successfully connected to MongoDB!");
    } finally {
        // Ensures that the client will close when you finish/error
        // await client.close();
    }
}
run().catch(console.dir);


app.get('/', (req, res) => {
    res.send('Anik portfolio server Running....')
})

app.listen(port, () => {
    console.log(`Example app listening on port ${port}`)
})