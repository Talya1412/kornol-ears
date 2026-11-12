# kornol-ears — KORNOL's realtime voice sidecar

The "ears + mouth" half of KORNOL's realtime voice system. Discord
enforces DAVE end-to-end encryption for voice (since 2026-03-01), and
no Python voice-receive stack can decrypt DAVE frames — so this
Node.js sidecar (discord.js + @discordjs/voice + @snazzah/davey) does
the listening and speaking, while KORNOL's brain (speech-to-text,
dialogue, text-to-speech) runs on her home VM. The two halves talk
over a Cloudflare WebSocket bus; this process joins a Discord voice
channel only when a session is explicitly summoned, streams what it
hears to the brain, and plays back the spoken replies.

Sessions run as GitHub Actions workflow dispatches
(`.github/workflows/voice-session.yml`): a runner starts, the ears
join the requested channel, greet, converse, and leave when the room
empties; a `results/summary.json` report is pushed back at the end.

Secrets (bot token, bus token) live only in this repo's Actions
secrets — never in the code.
