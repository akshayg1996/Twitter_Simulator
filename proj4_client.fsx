#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System
open System.Threading

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                
            }
            remote {
                helios.tcp {
                    port = 8778
                    hostname = localhost
                }
            }
        }")

let rand = System.Random()
let serverip = fsi.CommandLineArgs.[1] |> string
let serverport = fsi.CommandLineArgs.[2] |>string
let clid = "User"
let N = fsi.CommandLineArgs.[3] |> int
let addr = "akka.tcp://TwitterServer@" + serverip + ":" + serverport + "/user/ServerActor"
let system = ActorSystem.Create("TwitterClient", configuration)
let twitterServer = system.ActorSelection(addr)

let mutable usersList = []
let hashtags = [|"#grilled";"#newspaper";"#nervous";"#helicopter";"#essential";"#kamal";"#DOS";"#COP5615isgreat";"#UF";"#Gainesville";"#AKKA";"#FSharp";"#twitter";"#Fall2021";"#akshay";"#essentially";"#establish";"#fast";"#fat";"#fate";"#father";"#fault";"#female";"#regularly";"#regulate";"#regulation";"#reinforce";"#reject";"#relate";"#relation";"#relationship";"#relative";"#relatively";"#relax";"#release";"#relevant";"#seat";"#second";"#secret";"#seem";"#segment";"#seize";"#select";"#selection";"#suddenly";"#sue";"#suffer";"#sufficient";"#sugar";"#suggest";"#suggestion";"#suicide";"#suit";"#summer";"#surely";"#themselves";"#then";"#theory";"#therapy";"#there";"#therefore";"#these";"#they";"#thick";"#thin";"#thing";"#threaten";"#training";"#transfer";"#transform";"#transformation";"#transition";"#translate";"#transportation";"#travel";"#treat"|]
let words=[|"about";"above";"abroad";"absence";"careful";"carefully";"carrier";"carry";"case";"cash";"cast";"cat";"catch";"brush";"buck";"budget";"build";"building";"bullet";"bunch";"butter";"button";"buy";"deep";"deeply";"deer";"defeat";"defend";"defendant";"defense";"defensive";"deficit";"define";"definitely";"definition";"degree";"delay";"equal";"equally";"equipment";"era";"error";"escape";"especially";"essay";"essential";"essentially";"establish";"fast";"fat";"fate";"father";"fault";"favor";"favorite";"fear";"find";"finding";"fine";"finger";"finish";"fire";"firm";"first";"fish";"fishing";"fit";"fitness";"five";"fix";"flag";"flame";"flat";"flavor";"flee";"flesh";"flight";"float";"floor";"grab";"grade";"heat";"heaven";"heavily";"heavy";"heel";"height";"helicopter";"hell";"hello";"help";"helpful";"her";"here";"heritage";"hero";"herself";"hey";"hi";"instead";"institution";"institutional";"instruction";"instructor";"kitchen";"knee";"knife";"knock";"lake";"land";"limited";"line";"link";"lip";"list";"listen";"literally";"literary";"marketing";"marriage";"neither";"nerve";"nervous";"net";"network";"never";"nevertheless";"new";"newly";"news";"newspaper";"others";"otherwise";"ought";"our";"ourselves";"preparation";"prepare";"prescription";"presence";"rapidly";"rare";"rarely";"rate";"rather";"rating"|]
let randomTweetGen () = 
    let mutable t:string = " "
    t <- String.Concat(t," ",words.[rand.Next(0,words.Length)])
    t <- String.Concat(t," ",hashtags.[rand.Next(0,hashtags.Length)])
    t

let appendSubscriber rank (users: list<IActorRef>) =
    for i in 0 .. rank-1 do
        twitterServer <! "Subscribe|"+users.[rank-1].Path.Name+"|"+users.[i].Path.Name

let sndTweetServer (selfref: IActorRef) rank (users: list<IActorRef>) login= 
    let mutable loginstatus = login
    let mutable retweet = false
    let temp = rand.Next(0,100)
    if temp%50 = 25 then
        if login then
            loginstatus <- false
            twitterServer <! "Logout|"+selfref.Path.Name+"|"
        else
            loginstatus <- true
            twitterServer <! "Login|"+selfref.Path.Name+"|"
    elif temp = 31 then
        twitterServer <! "GetTweets|"+selfref.Path.Name+"|"
    elif temp = 32 then
        twitterServer <! "GetMentions|"+selfref.Path.Name+"|"
    elif temp = 33 then
        twitterServer <! "GetHashTags|"+selfref.Path.Name+"|"+hashtags.[rand.Next(0,hashtags.Length)]+"|"
    else
        if temp%10 =0 then
            retweet <- true
        if retweet then
            let tweetmsg = "ReTweet|"+selfref.Path.Name+"|"
            twitterServer <! tweetmsg
        else
            let randomtweet=randomTweetGen()
            let tweetmsg = "Tweet|"+selfref.Path.Name+"|" + "Hi @"+users.[rand.Next(0,N)].Path.Name+" "+randomtweet    
            twitterServer <! tweetmsg
    let finrnk = rank|>int
    system.Scheduler.ScheduleTellOnce(finrnk ,selfref,"TweetScheduler",selfref)
    loginstatus

type ClientType =
    | Start
    | StartResp
    | SubsResp
    

let ClientWorkerActor (mailbox:Actor<_>)=
    let mutable login = true
    let mutable rank = 0
    let mutable cid = 0
    let mutable serverRef = null

    let rec loop() = actor{
        let! msg = mailbox.Receive()
        let response =msg|>string
        let command = (response).Split '|'
        if command.[0].CompareTo("Zipf") = 0 then 
            rank <- command.[1] |> int
            twitterServer<!"Register|"+mailbox.Self.Path.Name
            twitterServer<!"Login|"+mailbox.Self.Path.Name
            serverRef <- mailbox.Sender()
        elif command.[0].CompareTo("Resp") = 0 then
            if command.[1].CompareTo("Register") = 0 then
                serverRef <! StartResp
            elif command.[1].CompareTo("Subscribe") = 0 then
                serverRef <! SubsResp
        elif command.[0].CompareTo("AppendSubscribers") = 0 then
            appendSubscriber rank usersList
        elif command.[0].CompareTo("TweetScheduler") = 0 then
            cid <- cid + 1
            login <- sndTweetServer mailbox.Self rank usersList login
        elif command.[0].CompareTo("Exit") = 0 then
            mailbox.Context.System.Terminate() |> ignore 
        elif command.[0].CompareTo("GetResp") = 0 then
            printfn "%A" msg
        return! loop()
    }
    loop()


usersList <- [for a in 1 .. N do yield(spawn system (clid + "_" + (string a)) ClientWorkerActor)]

let ClientBossActor (mailbox:Actor<_>)=
    let mutable startrespcount = 0
    let mutable tweetsack = 0
    let mutable subsrespcount = 0
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        match msg with 
        | Start ->      for i in 0 .. N-1 do
                            usersList.[i] <! "Zipf|"+(string (i+1))
        | StartResp ->  startrespcount <- startrespcount + 1
                        if startrespcount = N then
                            printfn "Created all users"
                            for i in 0 .. N-1 do
                                usersList.[i] <! "AppendSubscribers|"
        | SubsResp ->   subsrespcount <- subsrespcount + 1
                        if subsrespcount = N then
                            printfn "Users subscriptions finished"
                            for i in 0 .. N-1 do
                                usersList.[i] <! "TweetScheduler|"
         
        return! loop();
    }
    loop()

let clientRef = spawn system "ClientBossActor" ClientBossActor

clientRef <! Start

system.WhenTerminated.Wait()
