#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System
open System.Threading
open System.Data
open System.Collections.Generic


let mutable users = Map.empty
let mutable presentUsers = Map.empty
let mutable mentions = Map.empty
let mutable followers = Map.empty
let mutable hashTags = Map.empty
let mutable tweets = Map.empty
let mutable usersTweets = Map.empty
let mutable subscribersTweets = Map.empty

let config = 
    ConfigurationFactory.ParseString(
        @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                debug : {
                    receive : on
                    autoreceive : on
                    lifecycle : on
                    event-stream : on
                    unhandled : on
                }
            }
            remote {
                helios.tcp {
                    port = 8777
                    hostname = localhost
                }
            }
        }")


let system = ActorSystem.Create("TwitterServer", config)

let rand = System.Random()
let mutable baseUserID = 1234
let ticksPerMilsec = TimeSpan.TicksPerMillisecond |> float
let ticksPerMicsec = (TimeSpan.TicksPerMillisecond |> float )/(1000.0)


let combineList (list: List<string>) =
    let len = Math.Min(100,list.Count)
    let mutable resp = ""
    for i in 0 .. len-1 do
        resp <- resp+list.[i]+"|"
    resp

let userRegister username = 
    let userID = baseUserID |> string
    users <- users.Add(username,baseUserID|>string)
    baseUserID <- baseUserID + rand.Next(1,32)
    userID


let appendTweetUser userID tweet =
    let found = usersTweets.TryFind userID
    if found = None then
        let lst = new List<string>()
        lst.Add(tweet)
        usersTweets <- usersTweets.Add(userID,lst)
    else
        found.Value.Add(tweet)

let appendTweet tweetID tweet = 
    tweets <- tweets.Add(tweetID,tweet)

let isUserPresent userid = 
    let res = presentUsers.TryFind userid
    res <> None

let getUserID username =
    let found = users.TryFind username
    found

let getTweets userid =
    let resp = new List<string>()
    let tempList = usersTweets.TryFind userid
    if tempList <> None then
        tempList.Value
    else
        resp

let userLogin username clRef= 
    let mutable tempid = ""
    let mutable userid = getUserID username 
    if userid = None then
        tempid <- (userRegister username) 
    else
        tempid <- userid.Value
    let found = presentUsers.TryFind tempid
    if found = None then
        presentUsers <- presentUsers.Add(tempid,clRef)

let userLogout username = 
    let mutable userid = getUserID username
    if userid <> None then
        presentUsers <- presentUsers.Remove(userid.Value)

let appendHashtag hashtag tweet =
    let findHtg = hashTags.TryFind hashtag
    if findHtg = None then
        let tempList = new List<string>()
        tempList.Add(tweet)
        hashTags <- hashTags.Add(hashtag,tempList)
    else
        findHtg.Value.Add(tweet)

let getHashTags hashtag =
    let res = new List<string>()
    let htgList = hashTags.TryFind hashtag
    if htgList <> None then 
        for i in htgList.Value do
            res.Add(i)
    res

let getFollowers userid = 
    let followerList = followers.TryFind userid
    if followerList <> None then
        followerList.Value
    else
        let elist = new List<string>()
        elist

let appendFollower username followerUsername clRef=
    let userid = getUserID username
    if userid <> None then
        if (isUserPresent userid.Value) then
            let folPresID = getUserID followerUsername
            if folPresID<>None && userid <> None then
                let folList = followers.TryFind folPresID.Value
                if folList = None then
                    let followerList = new List<string>()
                    followerList.Add(userid.Value)
                    followers <- followers.Add(folPresID.Value,followerList)
                else 
                    folList.Value.Add(userid.Value)
                clRef <! "Resp|Subscribe|"
        else
            clRef <! "Resp|NotLoggedIn|"



let appendSubsTweet tweet userID =
    let followerList = getFollowers userID
    for i in followerList do
        let subsList = subscribersTweets.TryFind i        
        if subsList = None then
            let subsTweets = new List<string>()
            subsTweets.Add(tweet)
            subscribersTweets <- subscribersTweets.Add(i,subsTweets)
        else
            subsList.Value.Add(tweet)

let appendMentions userid tweet mentionusername =
    let taggedID = getUserID mentionusername
    if taggedID <> None then
        let mentionsList = mentions.TryFind taggedID.Value
        if mentionsList = None then
            let mutable tempMap = Map.empty
            let tempList = new List<string>()
            tempList.Add(tweet)
            tempMap <- tempMap.Add(userid,tempList)
            mentions <- mentions.Add(taggedID.Value,tempMap)
        else
            let user = mentionsList.Value.TryFind userid
            if user = None then
                let tempList = new List<string>()
                tempList.Add(tweet)
                let mutable tempMap = mentionsList.Value
                tempMap <- tempMap.Add(userid,tempList)
                mentions <- mentions.Add(taggedID.Value,tempMap)
            else
                user.Value.Add(tweet)

let getMentions userid = 
    let res = new List<string>()
    let tempList = mentions.TryFind userid
    if tempList <> None then
        for i in tempList.Value do
            for j in i.Value do
                res.Add(j)
    res

let sendTweetResp userid tweet = 
    let userPresentst = presentUsers.TryFind userid
    if userPresentst <> None then
        userPresentst.Value <! "Tweet|Self|"+tweet

let sendTweetsToActiveFollowers userid tweet =
    let followersList = getFollowers userid
    for i in followersList do
        let userPresentst = presentUsers.TryFind i
        if userPresentst <> None then
            userPresentst.Value <! "Tweet|Subscribe|"+tweet


type RegistrationType =
    | RegisterUser of string*IActorRef
    | Login of string*IActorRef
    | Logout of string*IActorRef

type TweetResolverType = 
    | Resolve of string*string*string*IActorRef

type FollowersType = 
    | Append of string*string*IActorRef
    | Update of string*string*IActorRef

type TweetsChangerType =
    | AppendTweet of string*string*string*IActorRef
    | ReTweet of string*IActorRef

type GetType =
    | GetTweets of string*IActorRef
    | GetTags of string*IActorRef
    | GetHashTags of string * string * IActorRef

exception Error1 of string


let GetActor(mailbox:Actor<_>)=
    let timer = System.Diagnostics.Stopwatch()
    let mutable gettweetscount =0
    let mutable gettagscount =0
    let mutable gethashtagscount =0
    let mutable gettweetstime = 0
    let mutable gethashtagstime = 0
    let mutable gettagstime = 0
    let rec loop()=actor{
        let! msg = mailbox.Receive()
        try
            match msg with
            | GetTweets(username,clRef) ->  timer.Restart()
                                            gettweetscount <- gettweetscount + 1
                                            let userid = getUserID username
                                            if userid <> None then
                                                if isUserPresent userid.Value then
                                                    let resp = (getTweets userid.Value) 
                                                    clRef <! "GetResp|GetTweets|"+(combineList resp)+"\n"
                                                else
                                                    clRef <! "Resp|NotLoggedIn|"
                                            gettweetstime <- gettweetstime + (timer.ElapsedTicks|>int)
                                            if gettweetscount%100 = 0 then
                                                printfn "Average time taken to process get tweets is %A microseconds after %i gettweets Requests and total %A milliseconds" ((gettweetstime|>float)/(ticksPerMicsec*(gettweetscount|>float))) gettweetscount ((gettweetstime|>float)/ticksPerMilsec)
            | GetTags(username,clRef)-> timer.Restart()
                                        gettagscount <- gettagscount + 1
                                        let userid = getUserID username
                                        if userid <> None then
                                            if isUserPresent userid.Value then
                                                let resp = (getMentions userid.Value)
                                                clRef <! "GetResp|GetMentions|"+(combineList resp)+"\n"
                                            else
                                                    clRef <! "Resp|NotLoggedIn|"
                                        gettagstime <- gettagstime + (timer.ElapsedTicks|>int)
                                        if gettagscount%100 = 0 then
                                                printfn "Average time taken to process get mentions is %A microseconds after %i getmentions Requests and total %A milliseconds" ((gettagstime|>float)/(ticksPerMicsec*(gettagscount|>float))) gettagscount ((gettagstime|>float)/ticksPerMilsec)
            | GetHashTags(username,hashtag,clRef)-> timer.Restart()
                                                    gethashtagscount <- gethashtagscount + 1
                                                    let userid = getUserID username
                                                    if userid <> None then
                                                        if isUserPresent userid.Value then
                                                            let resp = (getHashTags hashtag)
                                                            clRef <! "GetResp|GetHashTags|"+(combineList resp)+"\n"
                                                        else
                                                                clRef <! "Resp|NotLoggedIn|"
                                                    gethashtagstime <- gethashtagstime + (timer.ElapsedTicks|>int)
                                                    if gethashtagscount%100 = 0 then
                                                        printfn "Average time taken to process get hashtags is %A microseconds after %i gethashtags Requests and total %A milliseconds" ((gethashtagstime|>float)/(ticksPerMicsec*(gethashtagscount|>float))) gethashtagscount ((gethashtagstime|>float)/ticksPerMilsec)
        with
            | :? System.InvalidOperationException as ex ->  let tp = "ignore"
                                                            tp |>ignore
        return! loop()
    }
    loop()

let RegistrationActor (mailbox:Actor<_>)=
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        // printfn "%A" msg
        try
            match msg with 
            | RegisterUser(username,userref) ->     let mutable temp = ""
                                                    let tempid = getUserID username
                                                    if tempid = None then
                                                        temp <- userRegister username
                                                    else
                                                        temp <- tempid.Value
                                                    userref <! ("Resp|Register|"+(string temp))
            | Login(username,userref) ->    userLogin username userref
                                            userref <! "Resp|Login|"
            | Logout(username,userref) ->   userLogout username
                                            userref <! "Resp|Logout|"
        finally
            let tp = "Ignore"
            tp |> ignore
        return! loop()
    }
    loop()


let TweetsResolverActor (mailbox:Actor<_>) = 
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        try
            match msg with 
            | Resolve(userid,tweetid,tweet,clRef) ->let divider = (tweet).Split ' '
                                                    for i in divider do
                                                        if i.StartsWith "@" then
                                                            let temp = i.Split '@'
                                                            appendMentions userid tweet temp.[1]
                                                        elif i.StartsWith "#" then
                                                            appendHashtag i tweet
                                                    clRef <! "Resp|Tweet|"
        finally 
            let tp = "ignore"
            tp |> ignore
        return! loop()
    }
    loop()

let FollowersActor (mailbox:Actor<_>) = 
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        try
            match msg with
            | Append(user,follower,clRef) ->     appendFollower user follower clRef
                                            
            | Update (userID,tweet,clRef)->    appendSubsTweet tweet userID
        finally
            let tp = "ignore"
            tp |> ignore                                     
        return! loop()
    }
    loop()

let followersRef = spawn system "followerActor" FollowersActor
let tweetResolverRef = spawn system "TweetsResolverActor" TweetsResolverActor

let TweetsActor (mailbox:Actor<_>) =
    let timer = System.Diagnostics.Stopwatch()
    let mutable timetaken = 0.0
    let mutable millisecs = 0
    let mutable count = 0L
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        // printfn "Tweet Handler %A" msg
        try
            match msg with
            | AppendTweet(username,reqid,tweet,clRef)-> timer.Restart()
                                                        let userid = getUserID username
                                                        count <- count + 1L
                                                        if userid <> None then
                                                            if (isUserPresent userid.Value) then
                                                                let tweetid = count|>string
                                                                appendTweet tweetid tweet
                                                                appendTweetUser userid.Value tweet
                                                                tweetResolverRef <! Resolve(userid.Value, tweetid, tweet, clRef)
                                                                followersRef <! Update(userid.Value, tweet, clRef)
                                                                sendTweetResp userid.Value tweet
                                                                sendTweetsToActiveFollowers userid.Value tweet
                                                            else
                                                                clRef <! "Resp|NotLoggedIn|"
                                                        timetaken <- timetaken + (timer.ElapsedTicks|>float)
                                                        millisecs <- millisecs + (timer.ElapsedMilliseconds|>int)
                                                        if count % 10000L = 0L then                                                
                                                            printfn "Average Tweet processing after %i tweets is %A microseconds and total %A milliseconds" count (timetaken/(ticksPerMicsec*(count|>float))) (timetaken/ticksPerMilsec) 
                                                               
            | ReTweet(username, clRef)->let ind = rand.Next(0,count|>int)|>string
                                        let tweet = tweets.TryFind ind
                                        if tweet <> None then
                                            mailbox.Self<! AppendTweet(username,ind,tweet.Value,clRef)
        finally
            let tp = "Ignore "
            tp |> ignore
                    

        return! loop()
    }
    loop()

let tweetsRef = spawn system "tweetActor" TweetsActor
let registrationRef = spawn system "registrationActor" RegistrationActor
let getRef = spawn system "GetActor" GetActor


let commlink = 
    spawn system "ServerActor"
    <| fun mailbox ->
        let mutable ticks = 0L
        let mutable reqid = 0
        let timer = System.Diagnostics.Stopwatch()
        
        let rec loop() =
            actor {
                let! msg = mailbox.Receive()
                reqid <- reqid + 1
                timer.Restart()
                let command = (msg|>string).Split '|'
                if command.[0].CompareTo("Register") = 0 then
                    registrationRef <! RegisterUser(command.[1],mailbox.Sender())
                elif command.[0].CompareTo("Login") = 0 then
                    registrationRef <! Login(command.[1],mailbox.Sender())
                elif command.[0].CompareTo("Logout") = 0 then
                    registrationRef <! Logout(command.[1],mailbox.Sender())
                elif command.[0].CompareTo("Tweet") = 0 then
                    tweetsRef <! AppendTweet(command.[1],string reqid,command.[2],mailbox.Sender())
                elif command.[0].CompareTo("ReTweet") = 0 then
                    tweetsRef <! ReTweet(command.[1],mailbox.Sender())
                elif command.[0].CompareTo("Subscribe") = 0 then 
                    followersRef <! Append(command.[1],command.[2],mailbox.Sender())
                elif command.[0].CompareTo("GetTweets") = 0 then
                    getRef <! GetTweets(command.[1],mailbox.Sender())
                elif command.[0].CompareTo("GetMentions") = 0 then
                    getRef <! GetTags(command.[1],mailbox.Sender())
                elif command.[0].CompareTo("GetHashTags") = 0 then
                    getRef <! GetHashTags(command.[1],command.[2],mailbox.Sender())
                
                ticks <- ticks + timer.ElapsedTicks
                if reqid%10000 = 0 then
                    printfn "Time taken to handle %i requests from clients is %A milliseconds" reqid ((ticks|>float)/ticksPerMilsec)

                return! loop() 
            }
            
        loop()


printfn "Server Started"
system.WhenTerminated.Wait()