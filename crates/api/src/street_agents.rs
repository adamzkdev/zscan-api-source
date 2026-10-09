//! ZStreet's citizens as agents. Every zkSNARK has a fixed persona (name, job, temperament, take
//! on Zcash, home district), the same for every visitor. Each UTC day a cast of twelve lives on
//! the street; each new block is a turn: one model call (shared by every visitor) decides where
//! each of them goes next, in what mood, what they think, and which pairs meet and talk. What they
//! did earlier today is fed back in, so the day hangs together. With the model off, a template
//! moves them round from the block's facts.

use std::collections::{BTreeMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Mutex, OnceLock};

use axum::{
    extract::{ConnectInfo, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{Timelike, Utc};
use governor::DefaultKeyedRateLimiter;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::ApiError;
use crate::street::{self, BlockFacts, Flights};
use crate::street_llm::{self as llm, field};
use crate::AppState;

const NAMES: &[&str] = &[
    "Mika", "Yuna", "Kenji", "Amara", "Leo", "Ines", "Rafa", "Noor", "Sora", "Theo", "Lina",
    "Omar", "Hana", "Jules", "Priya", "Bruno", "Aiko", "Malik", "Zoe", "Ivan", "Nadia", "Kofi",
    "Elena", "Ren", "Sami", "Chloe", "Tariq", "Mei", "Felix", "Ayla", "Diego", "Rin", "Yara",
    "Hugo", "Lea", "Emeka", "Kaito", "Sofia", "Bilal", "Nina", "Arjun", "Maya", "Oskar", "Lucia",
    "Jin", "Farah", "Tomas", "Asha", "Paolo", "Rosa", "Kai", "Dalia", "Mateo", "Yusuf", "Clara",
    "Hiro", "Selin", "Andre", "Maud", "Idris", "Vera", "Ezra", "Luz", "Nico",
];

const JOBS: &[&str] = &[
    "barista at the corner café",
    "night-shift miner at the Proof Factory",
    "bus driver on the block line",
    "parcel courier for the MEMPOOL HQ",
    "street musician",
    "librarian at the Binhai Library",
    "trader on the Reserve's floor",
    "noodle-stall cook",
    "retired cryptographer",
    "art student",
    "security guard at the Museum",
    "taxi driver",
    "gardener in Central Park",
    "journalist for the ZStreet Daily",
    "pixel-art painter",
    "arcade owner in ZK Tower",
    "nurse",
    "skateboarder with no job",
    "wallet developer",
    "flower seller",
    "tour guide",
    "chef at the Marina hotel",
    "stadium groundskeeper",
    "pigeon whisperer",
];

const TRAITS: &[&str] = &[
    "cheerful and chatty",
    "grumpy but kind underneath",
    "anxious, checks the mempool too often",
    "dreamy and poetic",
    "competitive, bets on everything",
    "shy, speaks softly",
    "sarcastic",
    "endlessly curious",
    "calm and wise",
    "dramatic, everything is huge news",
    "a gossip who knows everyone",
    "a conspiracy theorist (harmless)",
];

const TAKES: &[&str] = &[
    "privacy is a human right, shielded by default",
    "still uses a transparent address and feels judged",
    "a Zcash OG since the Sprout days",
    "just learned what a viewing key is and loves it",
    "thinks every empty block is a bad omen",
    "loves Orchard and never stops saying so",
    "counts the days until NU7",
    "doesn't care about crypto, loves the city",
    "keeps a paper wallet in a sock",
    "wants everyone to shield their coins",
    "thinks miners are the real heroes",
    "is sure the zkSNARK on the statue winks at night",
];

const HOMES: &[&str] = &[
    "Midtown",
    "Parkside",
    "Uptown",
    "the West End",
    "the Finance district",
    "the Marina",
    "the South Bank",
    "the Port",
    "the shōtengai",
    "Downtown",
];

/// How they talk (the client pairs each with a greeting, same order).
const VOICES: &[&str] = &[
    "talks fast in run-on sentences and jumps from one topic to the next",
    "short, clipped sentences; never uses five words when two will do",
    "old-fashioned and very polite, calls everyone 'friend'",
    "Gen-Z slang ('lowkey', 'no cap', 'it's giving'), all lowercase energy",
    "sailor talk from years at the Port ('aye', 'landlubber', weather sayings)",
    "explains everything with food metaphors",
    "bone-dry deadpan, says absurd things with a straight face",
    "whispers and looks over their shoulder, as if everything were classified",
    "theatrical, speaks like a stage actor in a tragedy",
    "drops Japanese words ('ne', 'sugoi', 'yabai', 'maji de')",
    "answers questions with more questions",
    "narrates life like a sports commentator",
    "gentle and slow, with long pauses (...) and carefully chosen words",
    "blunt, no filter, says exactly what they think",
    "pedantic nerd who loves exact numbers and corrects people",
    "grandparent energy, calls you 'kiddo', rambles about the old days",
    "wildly enthusiastic, everything is THE BEST",
    "poetic, notices the light, the rain, the sounds of the street",
    "pushy salesperson, always pitching some deal",
    "film-noir detective, narrates in hard-boiled one-liners",
    "complains about everything, but in a funny way",
    "mixes French in ('bon', 'voila', 'oh la la', 'bref')",
    "laid-back surfer drawl ('dude', 'chill', 'gnarly')",
    "stiff bureaucrat who cites rules, forms and procedures",
    "distracted mumbler, always mid-thought about something else",
    "flirty and teasing, hands out cheeky compliments",
    "gamer who treats everything as a quest, a boss fight or a speedrun",
    "philosopher who turns small talk into big questions",
    "keeps talking to their imaginary cat, Byte, mid-sentence",
    "humble-bragger who name-drops the city's fanciest places",
    "anxious over-explainer who apologises constantly",
    "incurable pun-maker, can't resist a terrible pun",
];
const QUIRKS: &[&str] = &[
    "hums the bus horn tune without noticing",
    "counts things out loud",
    "is always eating something",
    "checks the mempool on a cracked old flip phone",
    "collects a bus ticket from every block",
    "never, ever sits down",
    "has named every pigeon on the avenue",
    "wears sunglasses at night",
    "writes down overheard conversations in a notebook",
    "taps the side of their head when thinking",
    "finishes other people's sentences, usually wrong",
    "is always cold, wrapped in a huge scarf",
    "salutes the zkSNARK of Liberty every time they pass it",
    "laughs at their own jokes before the punchline",
    "rates everything out of ten",
    "is forever losing their keys",
    "talks to plants",
    "does coin tricks with an old ZEC token",
    "won't step on the cracks in the pavement",
    "carries an umbrella even when it's dry",
    "snacks on persimmons stolen from the orchard",
    "stretches constantly, like a cat",
    "doodles QR codes on napkins",
    "flips a lucky coin before every decision",
    "photographs every sunset",
    "whistles whenever a new block lands",
    "quotes yesterday's ZStreet Daily headlines",
    "reads fortunes in the last digits of the block hash",
    "does push-ups at the bus stop",
    "reties their shoelaces when nervous",
    "always has fresh gossip about the mayor",
    "keeps a tiny bonsai in their coat pocket",
];
const PHRASES: &[&str] = &[
    "Shielded or nothing.",
    "The bus waits for no one.",
    "Not my block, not my problem.",
    "Every empty block is a nap.",
    "Trust the proof.",
    "Ten out of ten, no notes.",
    "Classic.",
    "The mempool remembers.",
    "Ask me again after the next block.",
    "Small fees, big dreams.",
    "Zero knowledge, full heart.",
    "That's above my pay grade.",
    "Keep your keys, keep your peace.",
    "Seventy-five seconds is a lifetime.",
    "Wild times on the avenue.",
    "Pigeons know things.",
    "If it's transparent, it's gossip.",
    "Sleep is for empty blocks.",
    "Rain's good for the neon.",
    "Don't tell the mayor.",
    "Proof or it didn't happen.",
    "Everyone's a miner on the inside.",
    "Life's a mempool, wait your turn.",
    "Big if true.",
    "Hold my persimmon.",
    "Orchard forever.",
    "I've seen worse.",
    "That's the spirit.",
    "Easy, tiger.",
    "Fair enough.",
    "We ride at dawn. Or at the next block.",
    "Not advice, just vibes.",
    "The city provides.",
    "Zero clue, zero knowledge.",
    "One block at a time.",
    "Chef's kiss.",
    "Absolutely not.",
    "Say less.",
    "Keep it shielded, keep it moving.",
    "Oh, it's on.",
];
const LOVES: &[&str] = &[
    "the 3 a.m. ramen in the shotengai",
    "the ZEC Flyer at sunset",
    "rainy nights and wet neon",
    "the warm hum of the Proof Factory",
    "jazz nights at the Opera",
    "the fattest koi in the Orchard pond",
    "a full bus leaving right on time",
    "old Sprout-era war stories",
    "melon soda from the vending machines",
    "karaoke in Downtown",
    "the view from the Needle's deck",
    "Block Stadium on match days",
    "secondhand books from the Binhai Library",
    "the bakery by the Marina",
    "fireworks after a big block",
    "the quiet of the persimmon orchard",
    "noodles so spicy they hurt",
    "the Colosseum's Proof Games",
    "a perfectly folded paper wallet",
    "watching the ferries cross the bay",
    "the street cats",
    "the arcade in ZK Tower",
    "iced matcha",
    "long tram rides with no destination",
    "the first bus of the morning",
    "clacky mechanical keyboards",
    "taking the long way home",
    "Big Ben striking midnight",
    "card games with strangers",
    "sunrise over the Golden Gate",
    "a heated argument",
    "people-watching from the fountain",
];
const HATES: &[&str] = &[
    "people who reuse transparent addresses",
    "empty blocks",
    "soggy noodles",
    "the selfie crowd at the Louvre pyramid",
    "the rush-hour bus crowd",
    "loud courier scooters",
    "anyone who says 'wen moon'",
    "pigeons, every last one",
    "wet socks",
    "slow walkers on the avenue",
    "the Reserve's stuffy lobby",
    "small talk about the weather",
    "losing umbrellas",
    "being called by the wrong name",
    "the vending machine that eats coins",
    "spoilers",
    "the mayor's decrees",
    "cold coffee",
    "queue-jumpers at the MEMPOOL shop",
    "flickering neon",
    "high fees",
    "the wind on the Golden Gate",
    "early mornings",
    "people who never shield",
    "pineapple on pizza",
    "the stadium's hot dogs",
    "lift music",
    "being rushed",
    "bad puns",
    "the Port at low tide",
    "the lift queue at the Needle",
    "running late for a block",
];
const SECRETS: &[&str] = &[
    "once fell asleep on the bus and rode three blocks before waking up",
    "is secretly terrified of pigeons",
    "wrote a love letter in a shielded memo and never shared the viewing key",
    "has never actually been up the Needle",
    "used to be a transparent-address maximalist",
    "sings in the shower at full volume",
    "keeps a diary of every empty block",
    "can't swim but works right by the water",
    "found a ZEC coin in the fountain and kept it",
    "is writing a novel about the city, and it's terrible",
    "swears they saw the zkSNARK of Liberty blink",
    "still sleeps with a nightlight",
    "has a second job nobody knows about",
    "is scared of the Pyramid's labyrinth",
    "never learned to ride a bike",
    "nods along but doesn't really understand zk-proofs",
    "cried at the Opera last week",
    "keeps their seed phrase in a cookie tin",
    "has a crush on someone who works at the Reserve",
    "got banned from the arcade for a week",
    "talks to the koi when they're sad",
    "keeps saying they'll leave the city, never will",
    "doesn't care about crypto, only about the people",
    "once lost a whole paper wallet in the laundry",
];
const DREAMS: &[&str] = &[
    "open a noodle bar on the avenue",
    "ride the very first NU7 bus",
    "climb the Eiffel Tower at night",
    "win the Proof Games",
    "get their portrait hung in the Museum",
    "sail out to the prison island",
    "run for mayor",
    "write the ZStreet Daily's front page",
    "see a block with a thousand transactions",
    "understand zk-proofs for real",
    "buy a flat in ZK Tower",
    "start a band that plays the Opera",
    "visit every landmark in a single day",
    "get the whole city to shield",
    "get a building of theirs voted up in City Works",
    "become a famous pixel artist",
    "open a cat cafe",
    "teach kids why privacy matters",
    "own their own taxi",
    "find the Pyramid's genesis chest",
    "throw the biggest party Block Stadium has seen",
    "travel beyond the bay",
    "become a librarian at Binhai",
    "retire to a flat by the Marina",
];
const AGES: &[&str] = &[
    "19",
    "22",
    "24",
    "27",
    "29",
    "31",
    "34",
    "38",
    "42",
    "47",
    "53",
    "61",
    "68",
    "74",
];

/// Where a citizen can go: the street's activity spots (the client knows each one), or home.
const SPOTS: &[(&str, &str)] = &[
    ("bench", "a bench on the plaza"),
    ("fountain", "the fountain in the Transparent square"),
    ("koi", "the koi pond in the Orchard garden"),
    ("market", "the market stalls of the square"),
    ("vending", "the vending machines on the avenue"),
    ("window", "the shop windows on the avenue"),
    ("bus", "the bus stop, to watch the next block arrive"),
    ("orchard", "the persimmon orchard"),
    ("home", "home, off the street for a while"),
];

/// What someone does at each spot, for the template.
const DOING: &[(&str, &str)] = &[
    ("bench", "resting on a bench"),
    ("fountain", "people-watching by the fountain"),
    ("koi", "feeding the koi"),
    ("market", "shopping at the stalls"),
    ("vending", "grabbing a drink"),
    ("window", "window shopping"),
    ("bus", "waiting for the next block"),
    ("orchard", "picking persimmons"),
    ("home", "heading home"),
];

const CAST: usize = 12;

pub(crate) fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn pick<'a>(list: &[&'a str], id: i32, salt: u64) -> &'a str {
    list[(mix(id as u64 * 131 + salt) % list.len() as u64) as usize]
}

/// Another citizen's name, never their own (same rule as the client).
fn other(id: i32, salt: u64, own: &str) -> &'static str {
    let mut i = (mix(id as u64 * 131 + salt) % NAMES.len() as u64) as usize;
    if NAMES[i] == own {
        i = (i + 1) % NAMES.len();
    }
    NAMES[i]
}

#[derive(Clone, Serialize)]
pub struct Persona {
    id: i32,
    pub(crate) name: &'static str,
    pub(crate) job: &'static str,
    #[serde(rename = "trait")]
    pub(crate) temper: &'static str,
    take: &'static str,
    home: &'static str,
    age: &'static str,
    /// How they talk.
    voice: &'static str,
    quirk: &'static str,
    phrase: &'static str,
    loves: &'static str,
    hates: &'static str,
    secret: &'static str,
    dream: &'static str,
    friend: &'static str,
    rival: &'static str,
}

pub(crate) fn persona(id: i32) -> Persona {
    Persona {
        id,
        name: pick(NAMES, id, 1),
        job: pick(JOBS, id, 2),
        temper: pick(TRAITS, id, 3),
        take: pick(TAKES, id, 4),
        home: pick(HOMES, id, 5),
        age: pick(AGES, id, 13),
        voice: pick(VOICES, id, 6),
        quirk: pick(QUIRKS, id, 7),
        phrase: pick(PHRASES, id, 8),
        loves: pick(LOVES, id, 9),
        hates: pick(HATES, id, 10),
        secret: pick(SECRETS, id, 11),
        dream: pick(DREAMS, id, 12),
        friend: other(id, 14, pick(NAMES, id, 1)),
        rival: other(id, 15, pick(NAMES, id, 1)),
    }
}

impl Persona {
    pub(crate) fn line(&self) -> String {
        format!(
            "#{} {} ({}): {} from {}; {}; talks like this: {}; habit: {}; catchphrase \"{}\"; loves {}, hates {}; best friend {}, rival {}; {}.",
            self.id,
            self.name,
            self.age,
            self.job,
            self.home,
            self.temper,
            self.voice,
            self.quirk,
            self.phrase,
            self.loves,
            self.hates,
            self.friend,
            self.rival,
            self.take
        )
    }
}

/// The day's cast: twelve zkSNARKs picked by the UTC date, no two with the same name.
pub(crate) fn cast_of(day: i64) -> Vec<i32> {
    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut k = 0u64;
    while ids.len() < CAST && k < 400 {
        let id = (mix(day as u64 * 1000 + k) % 10_000) as i32 + 1;
        k += 1;
        let n = persona(id).name;
        if ids.contains(&id) || names.contains(&n) {
            continue;
        }
        ids.push(id);
        names.push(n);
    }
    ids
}

#[derive(Clone, Serialize)]
struct AgentNow {
    id: i32,
    go: String,
    mood: String,
    thought: String,
    /// What they do there, in a few words ("feeding the koi").
    doing: String,
    /// What they say to a visitor walking past.
    greet: String,
    with: Option<i32>,
    /// Earlier today, newest last.
    journal: Vec<String>,
}

#[derive(Clone, Serialize)]
struct Talk {
    a: i32,
    b: i32,
    lines: Vec<String>,
}

/// The cast's day so far (kept in memory: a restart starts the day's story again).
struct Day {
    day: i64,
    agents: BTreeMap<i32, AgentNow>,
}

fn day_state() -> &'static Mutex<Day> {
    static S: OnceLock<Mutex<Day>> = OnceLock::new();
    S.get_or_init(|| {
        Mutex::new(Day {
            day: -1,
            agents: BTreeMap::new(),
        })
    })
}

/// Today's state, started fresh (everyone at home) on a new UTC day.
fn snapshot(day: i64) -> BTreeMap<i32, AgentNow> {
    let mut s = day_state().lock().unwrap();
    if s.day != day {
        s.day = day;
        s.agents = cast_of(day)
            .into_iter()
            .map(|id| {
                (
                    id,
                    AgentNow {
                        id,
                        go: "home".into(),
                        mood: "sleepy".into(),
                        thought: "A new day in ZStreet.".into(),
                        doing: "sleeping in".into(),
                        greet: "Morning! Early bird, huh?".into(),
                        with: None,
                        journal: Vec::new(),
                    },
                )
            })
            .collect();
    }
    s.agents.clone()
}

fn commit(day: i64, agents: &BTreeMap<i32, AgentNow>) {
    let mut s = day_state().lock().unwrap();
    if s.day == day {
        s.agents = agents.clone();
    }
}

/// How likely each spot is at this hour (UTC), for the template.
fn hour_weight(spot: &str, h: u32) -> u32 {
    let night = !(6..23).contains(&h);
    match spot {
        "home" if night => 6,
        "home" => 1,
        "bus" if (6..10).contains(&h) => 5,
        "market" if (11..14).contains(&h) => 5,
        "market" | "orchard" if night => 0,
        "window" if (18..23).contains(&h) => 4,
        _ => 2,
    }
}

/// Without the model: each citizen moves on by the hour, thinks a line about the block, and two
/// pairs share a spot and trade lines from the block's gossip.
fn template_turn(f: &BlockFacts, hour: u32, agents: &mut BTreeMap<i32, AgentNow>) -> Vec<Talk> {
    let lines: Vec<String> = street::gossip_template(f)
        .into_iter()
        .map(|l| l.text)
        .collect();
    let ids: Vec<i32> = agents.keys().copied().collect();
    for (i, id) in ids.iter().enumerate() {
        let r = mix(f.height as u64 * 7919 + *id as u64);
        let total: u32 = SPOTS.iter().map(|(s, _)| hour_weight(s, hour)).sum();
        let mut k = (r % total.max(1) as u64) as u32;
        let mut go = "bench";
        for (s, _) in SPOTS {
            let w = hour_weight(s, hour);
            if k < w {
                go = s;
                break;
            }
            k -= w;
        }
        let a = agents.get_mut(id).unwrap();
        a.go = go.into();
        a.with = None;
        a.mood = ["happy", "curious", "tired", "excited", "calm"][(r >> 8) as usize % 5].into();
        a.thought = lines[(i + (r >> 16) as usize) % lines.len()].clone();
        a.doing = DOING
            .iter()
            .find(|(k, _)| *k == go)
            .map_or("hanging around", |(_, d)| d)
            .into();
        a.greet = lines[(i + 3 + (r >> 20) as usize) % lines.len()].clone();
    }
    let mut talks = Vec::new();
    for pair in 0..2 {
        let (x, y) = (pair * 2, pair * 2 + 1);
        if y >= ids.len() || lines.len() < 3 {
            break;
        }
        let (a, b) = (ids[x], ids[y]);
        let go = agents[&a].go.clone();
        if go == "home" {
            continue;
        }
        agents.get_mut(&b).unwrap().go = go;
        agents.get_mut(&a).unwrap().with = Some(b);
        agents.get_mut(&b).unwrap().with = Some(a);
        let o = (f.height as usize + pair) % lines.len();
        talks.push(Talk {
            a,
            b,
            lines: (0..3)
                .map(|k| lines[(o + k) % lines.len()].clone())
                .collect(),
        });
    }
    talks
}

/// One model turn: where everyone goes, moods, thoughts, conversations. `None` when it fails.
async fn model_turn(
    s: &AppState,
    f: &BlockFacts,
    hour: u32,
    agents: &mut BTreeMap<i32, AgentNow>,
) -> Option<Vec<Talk>> {
    let cast: Vec<String> = agents
        .values()
        .map(|a| {
            let mut l = persona(a.id).line();
            l.push_str(&format!(
                " Now: {} ({}), thinking \"{}\".",
                a.go, a.mood, a.thought
            ));
            if !a.journal.is_empty() {
                l.push_str(&format!(" Earlier today: {}", a.journal.join(" / ")));
            }
            l
        })
        .collect();
    let spots: Vec<String> = SPOTS.iter().map(|(k, d)| format!("{k} = {d}")).collect();
    let facts = format!(
        "{}\nHour of the day (UTC): {hour}.\n\nPlaces:\n{}\n\nThe cast:\n{}",
        f.text(),
        spots.join("\n"),
        cast.join("\n")
    );
    let system = "You direct the citizens of ZStreet, a voxel city of pixel-art zkSNARK \
        characters where each mined Zcash block is a bus leaving the stop. A block was just \
        mined: it is a new turn. For every citizen of the cast decide where they go next (one \
        of the place keys), their mood (one word), what they do there (at most 40 characters, \
        e.g. \"arguing about fees with Kenji\"), a short first-person thought (at most 70 \
        characters) and what they would say to a stranger walking past them (at most 60 \
        characters), all in that citizen's own way of talking (their 'talks like this', habit and \
        catchphrase: two citizens must never sound alike), fitting their persona, what they did \
        earlier today and this block. Keep \
        each one's day coherent: people have reasons, follow up on earlier thoughts, make \
        friends and rivals, react to the block. Pair up four to six couples who go to the same \
        place and talk: write their conversation as 2 to 4 alternating lines, the first by a, \
        at most 60 characters each, each in their own voice, about the block, the city, each \
        other, their loves and grudges or their day; friends banter, rivals needle each other. Write in English only. Use only facts given: copy any number exactly as \
        written, never invent amounts, prices or events. No price predictions or financial \
        advice, no hashtags, no emojis. Return strict JSON only: \
        {\"agents\":[{\"id\":0,\"go\":\"bench\",\"mood\":\"happy\",\"doing\":\"...\",\"thought\":\"...\",\"greet\":\"...\"}],\
        \"talks\":[{\"a\":0,\"b\":0,\"lines\":[\"...\",\"...\"]}]}";
    let v = llm::complete_json(&s.http, system, &facts, 2200).await?;
    let mut seen = 0;
    for x in v["agents"].as_array().into_iter().flatten() {
        let Some(id) = x["id"].as_i64().map(|i| i as i32) else {
            continue;
        };
        let Some(a) = agents.get_mut(&id) else {
            continue;
        };
        let go = x["go"].as_str().unwrap_or("");
        if !SPOTS.iter().any(|(k, _)| *k == go) {
            continue;
        }
        let Some(thought) = field(&x["thought"], 110, &facts) else {
            continue;
        };
        a.go = go.into();
        a.mood = x["mood"]
            .as_str()
            .map(|m| llm::fit(m, 16).to_lowercase())
            .filter(|m| !m.is_empty() && m.chars().all(|c| c.is_alphabetic() || c == '-'))
            .unwrap_or_else(|| "fine".into());
        a.thought = thought;
        a.doing = field(&x["doing"], 60, &facts).unwrap_or_else(|| {
            DOING
                .iter()
                .find(|(k, _)| *k == go)
                .map_or("hanging around", |(_, d)| d)
                .into()
        });
        a.greet = field(&x["greet"], 100, &facts).unwrap_or_else(|| "Hey there!".into());
        a.with = None;
        seen += 1;
    }
    // a turn that moved almost nobody is a failed turn
    if seen < agents.len() / 2 {
        return None;
    }
    let mut talks = Vec::new();
    for t in v["talks"].as_array().into_iter().flatten().take(6) {
        let (Some(a), Some(b)) = (t["a"].as_i64(), t["b"].as_i64()) else {
            continue;
        };
        let (a, b) = (a as i32, b as i32);
        if a == b || !agents.contains_key(&a) || !agents.contains_key(&b) {
            continue;
        }
        if talks
            .iter()
            .any(|x: &Talk| [x.a, x.b].iter().any(|i| *i == a || *i == b))
        {
            continue;
        }
        let lines: Vec<String> = t["lines"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|l| field(l, 90, &facts))
            .take(4)
            .collect();
        if lines.len() < 2 {
            continue;
        }
        // they meet where the first one goes (not at home)
        let mut go = agents[&a].go.clone();
        if go == "home" {
            go = agents[&b].go.clone();
        }
        if go == "home" {
            go = "bench".into();
        }
        for (me, other) in [(a, b), (b, a)] {
            let x = agents.get_mut(&me).unwrap();
            x.go = go.clone();
            x.with = Some(other);
        }
        talks.push(Talk { a, b, lines });
    }
    Some(talks)
}

#[derive(Deserialize)]
pub struct AgentsQuery {
    h: Option<u64>,
}

/// `GET /v1/street/agents?h=`: the day's cast, each with their persona and what they do after
/// block `h` (the tip by default), and the conversations of that turn. One turn per block, shared
/// by every visitor.
pub async fn agents(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<AgentsQuery>,
) -> Result<Response, ApiError> {
    static LIMIT: OnceLock<DefaultKeyedRateLimiter<IpAddr>> = OnceLock::new();
    static TURNS: Flights<Value> = Flights::new(32);
    if let Some(wait) = crate::limited_for(
        LIMIT.get_or_init(|| street::per_minute(30)),
        &street::ip_of(peer, &headers),
    ) {
        return Ok(crate::too_many_requests(wait, "Slow down"));
    }
    let tip = street::tip(&s).await?;
    // only recent blocks get a turn: history is not replayed
    let h = q.h.unwrap_or(tip).min(tip).max(tip.saturating_sub(6));
    let cell = TURNS.cell(h);
    let v = cell
        .get_or_try_init(|| async {
            let f = street::block_facts(&s, h as i64)
                .await?
                .ok_or_else(|| ApiError::NotFound(format!("Block {h} isn't indexed")))?;
            let now = Utc::now();
            let day = now.timestamp() / 86_400;
            let hour = now.hour();
            let mut agents = snapshot(day);
            let before = agents.clone();
            let talks = match model_turn(&s, &f, hour, &mut agents).await {
                Some(t) => t,
                None => {
                    agents = before.clone();
                    template_turn(&f, hour, &mut agents)
                }
            };
            // what each did last turn goes in their journal (the last four)
            for (id, a) in agents.iter_mut() {
                let mut j: VecDeque<String> = before[id].journal.iter().cloned().collect();
                let b = &before[id];
                if b.go != "home" || b.thought != "A new day in ZStreet." {
                    j.push_back(format!("at {}, {}: {}", b.go, b.mood, b.thought));
                }
                while j.len() > 4 {
                    j.pop_front();
                }
                a.journal = j.into();
            }
            commit(day, &agents);
            let cast: Vec<Value> = agents
                .values()
                .map(|a| {
                    let mut v = serde_json::to_value(persona(a.id)).unwrap_or_default();
                    v["now"] = serde_json::to_value(a).unwrap_or_default();
                    v
                })
                .collect();
            Ok::<_, ApiError>(json!({ "height": h, "day": day, "cast": cast, "talks": talks }))
        })
        .await?;
    Ok(Json(v.clone()).into_response())
}

#[derive(Deserialize)]
pub struct PeopleQuery {
    ids: String,
}

/// `GET /v1/street/people?ids=1,2,3`: personas for up to 64 zkSNARKs (anyone you meet in the
/// city). A pure function of the id, cacheable for good.
pub async fn people(Query(q): Query<PeopleQuery>) -> Result<Response, ApiError> {
    let ids: Vec<i32> = q
        .ids
        .split(',')
        .filter_map(|x| x.trim().parse().ok())
        .filter(|i| (1..=10_000).contains(i))
        .take(64)
        .collect();
    if ids.is_empty() {
        return Err(street::bad_request(
            "ids: 1 to 64 zkSNARK numbers, comma separated",
        ));
    }
    let list: Vec<Persona> = ids.into_iter().map(persona).collect();
    let mut r = Json(list).into_response();
    r.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("public, max-age=86400"),
    );
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn personas_are_stable() {
        assert_eq!(persona(42).name, persona(42).name);
        assert_eq!(persona(42).job, persona(42).job);
    }

    #[test]
    fn cast_is_twelve_distinct_names() {
        for day in 20_000..20_030 {
            let c = cast_of(day);
            assert_eq!(c.len(), CAST);
            let mut n: Vec<_> = c.iter().map(|i| persona(*i).name).collect();
            n.sort_unstable();
            n.dedup();
            assert_eq!(n.len(), CAST);
            assert!(c.iter().all(|i| (1..=10_000).contains(i)));
        }
    }

    #[test]
    fn template_moves_everyone() {
        let f = BlockFacts::sample();
        let mut agents: BTreeMap<i32, AgentNow> = cast_of(20_000)
            .into_iter()
            .map(|id| {
                (
                    id,
                    AgentNow {
                        id,
                        go: "home".into(),
                        mood: "sleepy".into(),
                        thought: String::new(),
                        doing: String::new(),
                        greet: String::new(),
                        with: None,
                        journal: vec![],
                    },
                )
            })
            .collect();
        let talks = template_turn(&f, 12, &mut agents);
        assert!(agents.values().all(|a| !a.thought.is_empty()));
        assert!(agents
            .values()
            .all(|a| SPOTS.iter().any(|(k, _)| *k == a.go)));
        for t in &talks {
            assert_eq!(agents[&t.a].go, agents[&t.b].go);
            assert!(t.lines.len() >= 2);
        }
    }
}

