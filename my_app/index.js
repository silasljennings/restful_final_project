var express = require('express');
var app = express();
app.use(express.json());

const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
var ObjectId = require('mongodb').ObjectId; 


// Connection URL
const url = 'mongodb://omega.unasec.info';

// Database Name
const dbName = 'amazon';

// Create a new MongoClient
//const client = new MongoClient(url);
 MongoClient.connect(url, { useNewUrlParser: true }, function(err, db) {
    assert.equal(null, err);
    
    console.log("Connected successfully to server");
    const dbo = db.db(dbName);
    
  //returns a review identified by id  
  app.get('/reviews/find/:id', (req, res) => {
  
    var id = req.params.id; 
    var o_id = new ObjectId(id);
    dbo.collection("reviews").findOne({ "_id": o_id }, function(err, document){
    
    if (err) { return console.log("error: " + err);} 
      console.log(document);
      res.send(document);
      db.close();
    });
  });
  
  //get a random review by stars -- n sets the number of reviews to return and stars 
  //sets the star rating to match and return 
  app.get('/reviews/find/random/:n/:stars', (req, res) => {
  
    var n = parseInt(req.params.n); 
    var stars = parseInt(req.params.stars);
    dbo.collection("reviews").find({ "review.star_rating": stars }).limit(n).toArray(function(err, document){
    
    if (err) { return console.log("error: " + err);} 
      console.log(document);
      res.send(document);
      db.close();
    });
  });
  

  //get a random review by date -- n sets the number of reviews to return and from_date 
  //and to_date sets the dates to match for return 
  app.get('/reviews/find/random/:n/:from_date/:to_date', (req, res) => {
  
    var n = parseInt(req.params.n); 
    var from_date =  new Date(req.params.from_date).toISOString();
    var to_date = new Date(req.params.to_date).toISOString();
  
    dbo.collection("reviews").find({"review.date": {$gt: from_date, $lt: to_date}}).limit(n).toArray(function(err, document){
    
    if (err) { return console.log("error: " + err);} 
      console.log(document);
      res.send(document);
      db.close();
    });
  });
  
  //deletes a review identified by id  
  app.delete('/reviews/delete/:id', (req, res) => {

    var id = req.params.id; 
    var o_id = new ObjectId(id);

    dbo.collection("reviews").deleteOne({ "_id": o_id }, function(err, obj){
    
    if (err) { return console.log("error: " + err);} 
      res.send(obj.result.n + " document(s) deleted");
      db.close();
    });
  });
  

  //inserts a new item into the collection
  app.post('/reviews/post/:id', (req, res) => {

    var id = req.params.id; 
    var o_id = new ObjectId(id);
  
    //setting the reviewid of new review to the next record in the array
    let newDoc = {
        "_id" : o_id,
        "day" : "5",
        "marketplace" : "MI",
        "customer_id" : "83838383",
        "vine" : "N",
        "verified_purchase" : "Y",
        "review" : 
        {
          "id" : "R1VFX9Z8VDDDDD",
          "headline" : "One Stars",
          "body" : "Terrible",
          "star_rating" : "1",
          "date" : "2018-04-08T00:00:00Z"
        },
        "product":
        {
          "id" : "B00RU5555",
          "parent" : "994364111",
          "title" : "Auburn Football",
          "category" : "Sports"
        },
        "votes": 
        {
          "helpful_votes" : "0",
          "total_votes" : "0"
        }
    };
    
    dbo.collection("reviews").insertOne(newDoc, function(err, res) {
      if (err) throw err;
      console.log("1 document inserted");
      db.close();
    }); 
  });


  //updated the marketplace from US to CA -- document identified by id  
  app.put('/reviews/put/:id', (req, res) => {
  
    var id = req.params.id; 
    var o_id = new ObjectId(id);
  
    var newvalues = { $set: {marketplace: "CA" } };
    dbo.collection("reviews").updateOne({ "_id": o_id }, newvalues, function(err, res) {
    if (err) throw err;
      console.log("1 document updated");
      db.close();
    });
  });
  
  //returns an average star number for reviews over time - <YYYY-mm-dd>
  //avg review stars over time
app.get('/reviews/:from/:to', function (req, res) {          
    
      var from = new Date(`${req.params.from}`);
      var to = new Date(`${req.params.to}`);
  
      dbo.collection("reviews").aggregate([
        { $limit : 1000000 },
        {$match:
                { "review.date" : { $gte : from, $lte : to } } }, 
        {$group: 
            { 
                _id: null, 
                avgStars: { $avg : "$review.star_rating" } 
            }}]).toArray(function(err, results) { 
        // callback arguments are err or an array of results
      if(!err) {res.json(results);}
      else {
          res.send(err);
          db.close();
      }
    });
  });
  
//returns the avg review info for a customer by category
app.get('/reviews/find/info/:custid', function (req, res) {          
      dbo.collection("reviews").aggregate([
        {$match: { customer_id : `${req.params.custid}` } }, 
        {$group: { 
                _id: "$product.category", 
                avgStars: { $avg : "$review.star_rating" }, 
                avgHelpfulVotes: { $avg : "$votes.helpful_votes" },
                avgTotalVotes: { $avg : "$votes.total_votes" }
            }}]).toArray(function(err, results) { 
      if(!err) {res.json(results);}
      else {
        res.send(err);
        db.close();
      }
    });
  });


  //returns the average for helpful votes by product
app.get('/reviews/helpful/votes/:prodid', function (req, res) {          
      dbo.collection("reviews").aggregate([ 
        { $limit : 1000000 }, 
        { $match : { "product.id" : `${req.params.prodid}` }}, 
        { $group: 
            { 
              _id: null, 
              avgHelpfulVotes: { $avg : "$votes.helpful_votes" } 
            }}]).toArray(function(err, results) { 
      if(!err) {res.json(results);}
      else {
          res.send(err);
          db.close();
      }
    });
  });
  
const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Listening on port ${port}...`)); 
  
});

