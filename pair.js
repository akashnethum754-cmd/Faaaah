import express from 'express';
import fs from 'fs-extra';
import path from 'path';
import sharp from 'sharp';
import { exec } from 'child_process';
import mongoose from 'mongoose';
import moment from 'moment-timezone';
import https from 'https';
import axios from 'axios';
import dotenv from 'dotenv';
import yts from 'yt-search';
import { pipeline } from 'stream/promises';
import splitFileModule from 'split-file';
import crypto from 'node:crypto';
const { splitFile } = splitFileModule;
dotenv.config();

// ==========================================
// ⭐ SHAGGY XMD - Shared Helpers
// ==========================================

const parseSizeMB = (s) => {
    if (!s) return 0;
    const m = s.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
    if (!m) return 0;
    const v = parseFloat(m[1]);
    const u = m[2];
    if (u === 'GB') return v * 1024;
    if (u === 'MB') return v;
    return 0;
};

// ⭐ Google Drive — HTML form extraction
const downloadGdriveFile = async (url, dest) => {
    await fs.ensureDir(path.dirname(dest));

    const idMatch = url.match(/(?:id=|\/d\/|file\/d\/)([a-zA-Z0-9_-]+)/);
    if (!idMatch || !idMatch[1]) throw new Error('Invalid Google Drive URL');

    const fileId = idMatch[1];
    console.log(`[GDrive] File ID: ${fileId}`);

    const cookieJar = new Map();
    const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

    const addCookies = (setCookies = []) => {
        setCookies.forEach(c => {
            const [nameVal] = c.split(';');
            const [name, ...valParts] = nameVal.split('=');
            cookieJar.set(name.trim(), valParts.join('=').trim());
        });
    };

    const cookieHeader = () => Array.from(cookieJar.entries()).map(([k, v]) => `${k}=${v}`).join('; ');

    try {
        // Step 1: Get HTML page
        console.log(`[GDrive] Step 1: Fetching HTML...`);

        const firstRes = await axios.get(
            `https://drive.usercontent.google.com/download?id=${fileId}&export=download&authuser=0`,
            {
                responseType: 'text',
                timeout: 30000,
                maxRedirects: 5,
                headers: {
                    'User-Agent': UA,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9'
                },
                validateStatus: () => true
            }
        );

        addCookies(firstRes.headers['set-cookie']);

        const html = firstRes.data || '';
        console.log(`[GDrive] HTML length: ${html.length}`);

        // Step 2: Extract form action + inputs
        const actionMatch = html.match(/<form[^>]+action="([^"]+)"/);
        const formAction = actionMatch ? actionMatch[1].replace(/&amp;/g, '&') : null;

        const inputs = {};
        let m;

        const inputRegex1 = /<input[^>]+name="([^"]+)"[^>]+value="([^"]*)"/g;
        while ((m = inputRegex1.exec(html)) !== null) inputs[m[1]] = m[2];

        const inputRegex2 = /<input[^>]+value="([^"]*)"[^>]+name="([^"]+)"/g;
        while ((m = inputRegex2.exec(html)) !== null) inputs[m[2]] = m[1];

        console.log(`[GDrive] Form action: ${formAction ? 'found' : 'none'}`);
        console.log(`[GDrive] Inputs: ${JSON.stringify(Object.keys(inputs))}`);

        // Step 3: Build download URL
        let downloadUrl = '';

        if (formAction) {
            const params = new URLSearchParams();
            for (const [k, v] of Object.entries(inputs)) params.append(k, v);
            downloadUrl = `${formAction}?${params.toString()}`;
        } else {
            const params = new URLSearchParams({
                id: fileId,
                export: 'download',
                confirm: 't'
            });
            if (inputs.uuid) params.append('uuid', inputs.uuid);
            if (inputs.at) params.append('at', inputs.at);
            downloadUrl = `https://drive.usercontent.google.com/download?${params.toString()}`;
        }

        console.log(`[GDrive] Step 2: URL: ${downloadUrl.substring(0, 100)}...`);

        // Step 4: Download file
        const writer = fs.createWriteStream(dest);
        const fileRes = await axios({
            url: downloadUrl,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 10,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': UA,
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cookie': cookieHeader(),
                'Referer': `https://drive.usercontent.google.com/download?id=${fileId}&export=download`,
                'Origin': 'https://drive.usercontent.google.com'
            }
        });

        const ct = (fileRes.headers['content-type'] || '').toLowerCase();
        console.log(`[GDrive] Content-Type: ${ct}`);

        if (ct.includes('text/html')) {
            fileRes.data.destroy();
            throw new Error('Google Drive returned HTML again (blocked)');
        }

        fileRes.data.pipe(writer);

        await new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            fileRes.data.on('error', reject);
        });

        const stats = await fs.stat(dest);
        const sizeMB = stats.size / 1024 / 1024;
        console.log(`[GDrive] ✅ Downloaded: ${sizeMB.toFixed(1)} MB`);

        if (sizeMB < 1) {
            await fs.remove(dest).catch(() => {});
            throw new Error(`File too small: ${sizeMB.toFixed(2)} MB`);
        }

        return { success: true, size: stats.size };

    } catch (err) {
        console.error('[GDrive] ❌ Error:', err.message);
        throw err;
    }
};

// ⭐ Direct download (non-GDrive)
const downloadDirect = async (url, dest) => {
    await fs.ensureDir(path.dirname(dest));
    const STALL_MS = 120000; // 2 min: data enne nathi unoth download eka nawaththanawa
    const controller = new AbortController();
    const res = await axios({
        url,
        method: 'GET',
        responseType: 'stream',
        timeout: 120000,          // connect / first byte ekata 2 min
        signal: controller.signal,
        maxRedirects: 10,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*'
        }
    });

    const ct = (res.headers['content-type'] || '').toLowerCase();
    if (ct.includes('text/html')) {
        res.data.destroy();
        throw new Error('HTML response (not a file)');
    }

    const writer = fs.createWriteStream(dest);
    let stallTimer = null;
    const armStall = () => {
        clearTimeout(stallTimer);
        stallTimer = setTimeout(() => {
            controller.abort();
            res.data.destroy(new Error('Download stalled (2 min data nathi)'));
        }, STALL_MS);
    };
    armStall();
    res.data.on('data', armStall);

    try {
        res.data.pipe(writer);
        await new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
            res.data.on('aborted', () => reject(new Error('Download aborted')));
        });
    } catch (e) {
        writer.destroy();
        await fs.remove(dest).catch(() => {});
        throw e;
    } finally {
        clearTimeout(stallTimer);
    }

    const stats = await fs.stat(dest);
    if (stats.size < 1024 * 100) {
        await fs.remove(dest).catch(() => {});
        throw new Error(`File too small: ${(stats.size / 1024).toFixed(1)} KB`);
    }

    return { success: true, size: stats.size };
};

// ⭐ Smart downloader — auto pick GDrive or direct
const downloadSmart = async (url, dest) => {
    if (url.includes('drive.google.com') ||
        url.includes('drive.usercontent.google.com') ||
        url.includes('docs.google.com')) {
        return await downloadGdriveFile(url, dest);
    }
    return await downloadDirect(url, dest);
};

console.log('✅ SHAGGY XMD Helpers loaded');

// 🔁 Movie API slow/fail unoth ekaparak ayeth try karanawa (GET/HEAD witharai)
axios.interceptors.response.use(undefined, async (err) => {
    try {
        const cfg = err.config;
        if (!cfg || cfg.__retried) throw err;
        const method = (cfg.method || 'get').toLowerCase();
        if (method !== 'get' && method !== 'head') throw err;
        const status = err.response?.status;
        const transient = ['ECONNABORTED', 'ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN'].includes(err.code) || [429, 502, 503, 504].includes(status);
        if (!transient) throw err;
        cfg.__retried = true;
        await new Promise(r => setTimeout(r, 2000));
        return await axios(cfg);
    } catch (e) {
        return Promise.reject(e);
    }
});


import {
    default as makeWASocket,
    useMultiFileAuthState,
    delay,
    Browsers,
    fetchLatestBaileysVersion,
    downloadContentFromMessage,
    downloadMediaMessage,
    jidNormalizedUser,
    isPnUser
} from '@whiskeysockets/baileys';
// ==========================================================
// 🔘 BUTTON MODE ENGINE (inlined - separate file ekak one na)
// ==========================================================
import * as BaileysNS from '@whiskeysockets/baileys';
const { installButtonMode, sendList, extractIncoming, isReplyTo, sameChat, unwrap } = (() => {
const B = BaileysNS;
// ==========================================================
// 🔘 BUTTON MODE ENGINE
// ----------------------------------------------------------
// BUTTON_MODE = 'true'  -> numbered "reply a number" messages are sent as
//                          tappable buttons / list menus automatically.
// BUTTON_MODE = 'false' -> everything works the old way (reply with number).
//
// How it works (no command has to be rewritten):
//  1. socket.sendMessage is wrapped. When button mode is ON and a text/image
//     caption contains a numbered list (1, 2, 3 ...), it is sent as an
//     interactive message (quick-reply buttons for <=3 items, list otherwise).
//  2. When the user taps a button, we turn it into a normal "reply with number"
//     message (same stanzaId) and re-emit it, so the existing reply listeners
//     (menu, cineverse, cinesubz ...) work unchanged.
//  3. If anything fails, the original plain message is sent (fail-safe).
// ==========================================================

const cut = (s, n) => {
    s = String(s || '').replace(/\s+/g, ' ').trim();
    return s.length > n ? s.slice(0, n - 1).trimEnd() + '…' : s;
};
const num = (j) => String(j || '').split('@')[0].split(':')[0];
const isGroupJid = (j) => String(j || '').endsWith('@g.us');
const rid = () => 'BTNSYN' + Math.random().toString(36).slice(2, 12).toUpperCase();

// Commands that can run straight from a menu button (no argument needed)
const NO_ARG_CMDS = new Set([
    'alive', 'menu', 'help', 'ping', 'owner', 'license', 'bots', 'getkey',
    'groupinfo', 'jid', 'autorep', 'set', 'tagall', 'news', 'button'
]);

// ---------- incoming message helpers ----------

function unwrap(message) {
    let m = message || {};
    for (let i = 0; i < 6; i++) {
        const inner = m.ephemeralMessage?.message
            || m.viewOnceMessage?.message
            || m.viewOnceMessageV2?.message
            || m.viewOnceMessageV2Extension?.message
            || m.documentWithCaptionMessage?.message;
        if (!inner) break;
        m = inner;
    }
    return m;
}

/** Reads text / button id / quoted-message id from ANY message type. */
function extractIncoming(mek) {
    const m = unwrap(mek?.message);
    let text = '', buttonId = '', ctx;
    if (m.conversation) {
        text = m.conversation;
    } else if (m.extendedTextMessage) {
        text = m.extendedTextMessage.text || '';
        ctx = m.extendedTextMessage.contextInfo;
    } else if (m.buttonsResponseMessage) {
        buttonId = m.buttonsResponseMessage.selectedButtonId || '';
        ctx = m.buttonsResponseMessage.contextInfo;
    } else if (m.listResponseMessage) {
        buttonId = m.listResponseMessage.singleSelectReply?.selectedRowId || '';
        ctx = m.listResponseMessage.contextInfo;
    } else if (m.templateButtonReplyMessage) {
        buttonId = m.templateButtonReplyMessage.selectedId || '';
        ctx = m.templateButtonReplyMessage.contextInfo;
    } else if (m.interactiveResponseMessage) {
        const r = m.interactiveResponseMessage;
        try { buttonId = JSON.parse(r.nativeFlowResponseMessage?.paramsJson || '{}').id || ''; } catch { /* ignore */ }
        ctx = r.contextInfo;
    }
    return { text: String(text).trim(), buttonId, stanzaId: ctx?.stanzaId, ctx };
}

/** Is this message in the same chat as `jid`? (ignores :device and lid/pn mix-ups) */
function sameChat(key, jid) {
    const t = num(jid);
    return num(key?.remoteJid) === t || num(key?.remoteJidAlt) === t;
}

/**
 * True when `mek` answers the bot message `botMsgId`.
 * Accepts: a proper quoted reply, OR a bare number typed without quoting
 * (only when it does not quote some other message).
 */
function isReplyTo(mek, botMsgId, { allowBareNumber = true } = {}) {
    const { text, stanzaId } = extractIncoming(mek);
    if (stanzaId) return stanzaId === botMsgId;
    return allowBareNumber && /^\d{1,3}$/.test(text);
}

// ---------- numbered-list parser ----------

const DESC_START = /^[↳└⤷➥⮡]/;

function parseItems(raw) {
    const items = [];
    let expected = 1;
    for (const rawLine of String(raw || '').split('\n')) {
        // strip markdown + box characters
        const line = rawLine.replace(/[*_~`]/g, '').replace(/[│┃║]/g, ' ').trim();
        if (!line) continue;

        if (DESC_START.test(line)) {
            const last = items[items.length - 1];
            if (last && !last.description) last.description = line.replace(DESC_START, '').trim();
            continue;
        }
        const m = line.match(/^[^\p{L}\p{N}]*?(\d{1,3})(?:\uFE0F?\u20E3)?(?![\d%])(?!\.\d)\s*([.)\]:\-–—❭>»➜➤→]*)\s*(.*)$/u);
        if (!m) continue;
        if (parseInt(m[1], 10) !== expected) continue;

        let title = m[3].replace(/^[\s.)\]:\-–—❭>»➜➤→┃|]+/, '').trim();
        if (!title) title = `Option ${expected}`;
        items.push({ n: expected, title, description: '' });
        expected++;
        if (items.length >= 100) break;
    }
    return items;
}

// ---------- interactive senders ----------

const makeId = (socket) =>
    (B.generateMessageIDV2 ? B.generateMessageIDV2(socket.user?.id) : B.generateMessageID());

async function relayInteractive(socket, jid, { body, footer = '', image, buttons, quoted, msgId }) {
    const IM = B.proto.Message.InteractiveMessage;

    let header = IM.Header.create({ title: '', hasMediaAttachment: false });
    if (image) {
        try {
            const media = await B.prepareWAMessageMedia({ image }, { upload: socket.waUploadToServer });
            header = IM.Header.create({ title: '', hasMediaAttachment: true, imageMessage: media.imageMessage });
        } catch (e) {
            console.error('[ButtonMode] header image failed, sending without image:', e.message);
        }
    }

    const interactive = IM.create({
        body: IM.Body.create({ text: String(body).slice(0, 4000) }),
        footer: footer ? IM.Footer.create({ text: cut(footer, 60) }) : undefined,
        header,
        nativeFlowMessage: IM.NativeFlowMessage.create({ buttons, messageParamsJson: '' })
    });

    if (quoted?.key && quoted.message) {
        interactive.contextInfo = {
            stanzaId: quoted.key.id,
            participant: quoted.key.participant || quoted.key.remoteJid,
            quotedMessage: quoted.message
        };
    }

    const message = {
        viewOnceMessage: {
            message: {
                messageContextInfo: { deviceListMetadata: {}, deviceListMetadataVersion: 2 },
                interactiveMessage: interactive
            }
        }
    };

    const additionalNodes = [{
        tag: 'biz', attrs: {},
        content: [{
            tag: 'interactive', attrs: { type: 'native_flow', v: '1' },
            content: [{ tag: 'native_flow', attrs: { v: '9', name: 'mixed' }, content: [] }]
        }]
    }];
    if (!isGroupJid(jid)) additionalNodes.push({ tag: 'bot', attrs: { biz_bot: '1' } });

    await socket.relayMessage(jid, message, { messageId: msgId, additionalNodes });
    return { key: { remoteJid: jid, fromMe: true, id: msgId }, message, status: 1 };
}

const quickReply = (title, id) => ({
    name: 'quick_reply',
    buttonParamsJson: JSON.stringify({ display_text: cut(title, 20), id })
});

const singleSelect = (buttonText, sections) => ({
    name: 'single_select',
    buttonParamsJson: JSON.stringify({ title: cut(buttonText, 24), sections })
});

const chunkRows = (rows, size = 10) => {
    const out = [];
    for (let i = 0; i < rows.length; i += size) {
        const part = rows.slice(i, i + size);
        out.push({
            title: rows.length > size ? `${i + 1} - ${i + part.length}` : 'Choose one',
            highlight_label: '',
            rows: part
        });
    }
    return out;
};

/**
 * Send a tappable list. Row ids may contain the placeholder {ID}, which is
 * replaced with this message's own id (used for "back" rows).
 * rows: [{ title, description, id }]
 */
async function sendList(socket, jid, { text, image, footer, buttonText = '📋 Select', rows, quoted }) {
    const msgId = makeId(socket);
    const fixed = rows.slice(0, 100).map((r) => ({
        header: '',
        title: cut(r.title, 24),
        description: cut(r.description || '', 72),
        id: String(r.id).replace('{ID}', msgId)
    }));
    return relayInteractive(socket, jid, {
        body: text, footer, image, quoted, msgId,
        buttons: [singleSelect(buttonText, chunkRows(fixed))]
    });
}

// ---------- install ----------

/**
 * @param socket   baileys socket
 * @param opts.isOn    () => boolean   live button-mode flag
 * @param opts.prefix  () => string    live command prefix
 */
function installButtonMode(socket, { isOn, prefix }) {
    if (socket.__buttonModeInstalled) return;
    socket.__buttonModeInstalled = true;

    const origSend = socket.sendMessage.bind(socket);

    // ---- 1) outgoing: numbered lists -> buttons -------------------------
    socket.sendMessage = async (jid, content, options) => {
        try {
            if (isOn() && content && typeof content === 'object'
                && jid !== 'status@broadcast' && !String(jid).endsWith('@newsletter')) {
                const converted = await tryConvert(jid, content, options);
                if (converted) return converted;
            }
        } catch (e) {
            console.error('[ButtonMode] fallback to plain message:', e.message);
        }
        return origSend(jid, content, options);
    };

    async function tryConvert(jid, content, options) {
        const blocked = ['buttons', 'sections', 'templateButtons', 'interactiveMessage', 'react', 'delete',
            'edit', 'forward', 'poll', 'contacts', 'location', 'document', 'video', 'audio', 'sticker'];
        if (blocked.some((k) => content[k] !== undefined)) return null;

        const isText = typeof content.text === 'string';
        const isImage = content.image && typeof content.caption === 'string';
        if (!isText && !isImage) return null;

        const text = isText ? content.text : content.caption;
        const items = parseItems(text);
        if (items.length < 2) return null;

        const msgId = makeId(socket);
        const idFor = (n) => `BTN|NUM|${msgId}|${n}`;
        let buttons;
        if (items.length <= 3) {
            buttons = items.map((it) => quickReply(`${it.n}. ${it.title}`, idFor(it.n)));
        } else {
            const rows = items.map((it) => ({
                header: '',
                title: cut(`${it.n}. ${it.title}`, 24),
                description: cut(it.description || (it.title.length > 24 ? it.title : ''), 72),
                id: idFor(it.n)
            }));
            buttons = [singleSelect('📋 Select', chunkRows(rows))];
        }

        return relayInteractive(socket, jid, {
            body: text,
            image: isImage ? content.image : undefined,
            quoted: options?.quoted,
            buttons,
            msgId
        });
    }

    // ---- 2) incoming: button taps -> synthetic normal messages -----------
    socket.ev.on('messages.upsert', async ({ messages }) => {
        for (const mek of messages || []) {
            try {
                if (!mek?.message || mek.__synthetic) continue; // fromMe allowed: bot number owner taps buttons on own account
                const { buttonId } = extractIncoming(mek);
                if (!buttonId || !buttonId.startsWith('BTN|')) continue;

                const [, kind, a, b] = buttonId.split('|');
                const base = {
                    key: {
                        remoteJid: mek.key.remoteJid,
                        remoteJidAlt: mek.key.remoteJidAlt,
                        participant: mek.key.participant,
                        fromMe: !!mek.key.fromMe,
                        id: rid()
                    },
                    pushName: mek.pushName,
                    messageTimestamp: Math.floor(Date.now() / 1000),
                    __synthetic: true
                };
                const emit = (message) =>
                    socket.ev.emit('messages.upsert', { type: 'notify', messages: [{ ...base, message }] });

                if (kind === 'NUM') {
                    // tapped a numbered option -> behave like "reply with number"
                    emit({
                        extendedTextMessage: {
                            text: String(b),
                            contextInfo: {
                                stanzaId: a,
                                participant: socket.user?.id ? B.jidNormalizedUser(socket.user.id) : undefined,
                                quotedMessage: { conversation: '' }
                            }
                        }
                    });
                } else if (kind === 'CMD') {
                    const P = prefix();
                    if (NO_ARG_CMDS.has(a)) {
                        emit({ conversation: `${P}${a}` });
                    } else {
                        await origSend(mek.key.remoteJid, {
                            text: `✍️ *${P}${a}* use karanna:\n\`${P}${a} <name>\`\n\n_Command eka type karala name eka dala send karanna._`
                        }, { quoted: mek });
                    }
                } else if (kind === 'RUN') {
                    emit({ conversation: buttonId.slice('BTN|RUN|'.length) });
                }
            } catch (e) {
                console.error('[ButtonMode] tap handler error:', e.message);
            }
        }
    });

    console.log('🔘 Button mode engine installed');
}

return { installButtonMode, sendList, extractIncoming, isReplyTo, sameChat, unwrap };
})();

export const router = express.Router();
if (process.env.ALLOW_INSECURE_TLS === 'true') process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

const insecureAgent = new https.Agent({
    rejectUnauthorized: false
});
const config = {
    AUTO_RECORDING: 'false',
    AUTO_TYPING: 'false',
    AUTO_REACT: 'false',
    READ_CMD: 'false',
    BUTTON_MODE: 'false',
    API_MAIN_URL: 'https://zara.laksidu.site',
    API_MAIN_URL2:'https://zara.laksidu.site',
    API_CINESUBZ_URL:'https://api-siteh-22e22e4cb068.herokuapp.com',
    API_MOVIE_URL: 'https://api-siteh-22e22e4cb068.herokuapp.com',
    API_KEY:'lakiya_2f3b6c382d1236ad7a08d56331fb679935d51dfc846df2c254093fd1fff9494e',
    BOT_IMAGE:'https://files.catbox.moe/wogma6.jpeg',
    BOT_FOOTER:"SHAGGY XMD 〽️ᴏᴠɪᴇ Bᴏᴛ ᴠ4",
    MGROUP_LINK: 'https://chat.whatsapp.com/EeMhcQufXDFABM1MnR05Wh?s=cl&p=a&mlu=4&ilr=4',
    MOVIE_FOOTER:"⏤͟͟͞͞★❮ SHAGGY XMD 〽️OVIE ⏤͟͟͞͞★",
    MOVIE_CAPTION:"🇸‌ʜᴀɢɢY-xᴍᴅ ᴍᴏᴠɪᴇ 🔥🌈",
    PREFIX: '.',
    OWNER_NUMBERS: ['94784224161'],   // 🆕 ඔයාගේ number එක දාන්න
    BOT_NAME: "SHAGGY XMD",
    AIR_FOOTER: "ꜱʜᴀɢɢY-xᴍᴅ ᴠ2⚡",
    MODE: 'public',
    MAX_RETRIES: 3
};
const activeSockets = new Map();
// 🆕 Interval/Stream tracking - number ekakata interval/stream ekakma witharak thiyenna one (reconnect wela stack wenna epa)
const configSyncIntervals = new Map();
const configSyncStreams = new Map();
const presenceIntervals = new Map();

function clearNumberIntervals(sanitizedNumber) {
    if (configSyncIntervals.has(sanitizedNumber)) {
        clearInterval(configSyncIntervals.get(sanitizedNumber));
        configSyncIntervals.delete(sanitizedNumber);
    }
    if (configSyncStreams.has(sanitizedNumber)) {
        try { configSyncStreams.get(sanitizedNumber).close(); } catch (e) {}
        configSyncStreams.delete(sanitizedNumber);
    }
    if (presenceIntervals.has(sanitizedNumber)) {
        clearInterval(presenceIntervals.get(sanitizedNumber));
        presenceIntervals.delete(sanitizedNumber);
    }
    if (autoReplyStreams.has(sanitizedNumber)) {
        try { autoReplyStreams.get(sanitizedNumber).close(); } catch (e) {}
        autoReplyStreams.delete(sanitizedNumber);
    }
}

// 🆕 Auto-reply rules number ekakata memory eke cache karagannawa - hama message ekakama DB query
// ekak yawanna one nathuwa (eka message delay walata prashnayak una)
const autoReplyCache = new Map(); // key: sanitizedNumber -> Map(keyword -> {reply, image})
const autoReplyStreams = new Map();

async function loadAutoReplyCache(sanitizedNumber) {
    try {
        const rules = await AutoReply.find({ number: sanitizedNumber }).lean();
        const ruleMap = new Map();
        rules.forEach(r => ruleMap.set(r.keyword, { reply: r.reply || '', image: r.image || '' }));
        autoReplyCache.set(sanitizedNumber, ruleMap);
    } catch (err) {
        console.error(`Failed to load auto-reply cache for ${sanitizedNumber}:`, err.message);
    }
}

// 🆕 Anti-Delete feature ekata message content cache karagannawa (memory eke witharai, temporary)
const messageCache = new Map(); // key: `${remoteJid}:${msgId}` -> { text, senderNumber, timestamp }
const MESSAGE_CACHE_LIMIT = 500;

const rawCache = new Map(); // `${jid}:${id}` -> { msg, ... }  (media / text okkoma)
function cacheRaw(msg) {
    rawCache.set(`${msg.key.remoteJid}:${msg.key.id}`, {
        msg,
        pushName: msg.pushName,
        remoteJid: msg.key.remoteJid,
        remoteJidAlt: msg.key.remoteJidAlt,
        participant: msg.key.participant,
        participantAlt: msg.key.participantAlt,
        ts: Date.now()
    });
    if (rawCache.size > 800) rawCache.delete(rawCache.keys().next().value);
}

function cacheMessage(remoteJid, msgId, data) {
    const key = `${remoteJid}:${msgId}`;
    messageCache.set(key, { ...data, timestamp: Date.now() });
    if (messageCache.size > MESSAGE_CACHE_LIMIT) {
        const oldestKey = messageCache.keys().next().value;
        messageCache.delete(oldestKey);
    }
}
const socketCreationTime = new Map();
const SESSION_BASE_PATH = './session';
const NUMBER_LIST_PATH = './numbers.json';

// ==========================================
// MONGOOSE SCHEMAS
// ==========================================
const SessionSchema = new mongoose.Schema({
    number: { type: String, unique: true, required: true },
    creds: { type: Object, required: true },
    config: { type: Object },
    updatedAt: { type: Date, default: Date.now }
});
const Session = mongoose.model('Session', SessionSchema);

// 🆕 Auto Reply Schema (number ekakata scope wela, image ekath support karanawa)
const AutoReplySchema = new mongoose.Schema({
    number: { type: String, required: true, index: true },
    keyword: { type: String, required: true, lowercase: true },
    reply: { type: String, default: '' },
    image: { type: String, default: '' },
    createdAt: { type: Date, default: Date.now }
});
AutoReplySchema.index({ number: 1, keyword: 1 }, { unique: true });
const AutoReply = mongoose.model('AutoReply', AutoReplySchema);

async function connectMongoDB() {
    try {
        const mongoUri = process.env.MONGO_URI;
        await mongoose.connect(mongoUri);
        console.log(`
╔══════════════════════════════════════╗
║  ✅ MongoDB Connected Successfully   ║
║  ⚡ System Status : ONLINE           ║
╚══════════════════════════════════════╝
`);
    } catch (error) {
        console.error('MongoDB connection failed:', error);
        process.exit(1);
    }
}
connectMongoDB();

if (!fs.existsSync(SESSION_BASE_PATH)) {
    fs.mkdirSync(SESSION_BASE_PATH, { recursive: true });
}

function initialize() {
    activeSockets.clear();
    socketCreationTime.clear();
    console.log('Cleared active sockets and creation times on startup');
}

async function autoReconnectOnStartup() {
    try {
        let numbers = [];
        if (fs.existsSync(NUMBER_LIST_PATH)) {
            numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
            console.log(`Loaded ${(numbers.length)} numbers from numbers.json`);
        } else {
            console.warn('No numbers.json found, checking MongoDB for sessions...');
        }

        const sessions = await Session.find({}, 'number').lean();
        const mongoNumbers = sessions.map(s => s.number);
        console.log(`Found ${mongoNumbers.length} numbers in MongoDB sessions`);

        numbers = [...new Set([...numbers, ...mongoNumbers])];
        if (numbers.length === 0) {
            console.log('No numbers found in numbers.json or MongoDB, skipping auto-reconnect');
            return;
        }

        console.log(`Attempting to reconnect ${numbers.length} sessions...`);
        for (const number of numbers) {
            if (activeSockets.has(number)) {
                console.log(`Number ${number} already connected, skipping`);
                continue;
            }
            const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
            try {
                await EmpirePair(number, mockRes);
                console.log(`Initiated reconnect for ${number}`);
            } catch (error) {
                console.error(`Failed to reconnect ${number}:`, error);
            }
            await delay(1000);
        }
    } catch (error) {
        console.error('Auto-reconnect on startup failed:', error);
    }
}

initialize();
setTimeout(autoReconnectOnStartup, 5000);

// 🖼️ Bot images (menu eka witharak wenama image ekak use karanawa - menuImage)
const BOT_IMAGES = [
    'https://cloud.laksidu.site/stream/coNOXYjWqD/IMG-20260929-WA1350.jpg',
    'https://cloud.laksidu.site/stream/w2M1YwqSMp/IMG-20260929-WA7000.jpg',
    'https://cloud.laksidu.site/stream/aHdIQEq804/IMG-20260929-WA4539.jpg',
    'https://cloud.laksidu.site/stream/LWsBZ1wzUC/IMG-20260929-WA0563.jpg',
    'https://cloud.laksidu.site/stream/AjvgPH44oY/IMG-20260929-WA8438.jpg'
];
function pickBotImage(sc) {
    const custom = sc && sc.BOT_IMAGE;
    if (custom && custom !== config.BOT_IMAGE) return custom; // .set BOT_IMAGE kala nam eka
    return BOT_IMAGES[Math.floor(Math.random() * BOT_IMAGES.length)];
}

// ==========================================================
// 🎬 CineSubz download helpers (laksidu API 500 dunnoth chamindu API ekata fallback wenawa)
// ==========================================================
const CINE_FB_BASE = 'https://api.chamindu.site';
const CINE_FB_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';
const cineNorm = (t) => String(t || '').toLowerCase().replace(/[^a-z0-9]/g, '');

async function cineGetWithRetry(url) {
    try {
        return await axios.get(url, { timeout: 120000 });
    } catch (e) {
        const st = e.response?.status;
        if (!st || st >= 500) { // server error / network -> ekaparak ayeth try
            await new Promise(r => setTimeout(r, 3000));
            return await axios.get(url, { timeout: 120000 });
        }
        throw e;
    }
}

async function cineFallbackMovieLink(movieUrl, quality, index) {
    const r = await axios.get(`${CINE_FB_BASE}/api/v1/movie/cinesubz/infodl?q=${encodeURIComponent(movieUrl)}&api_key=${CINE_FB_KEY}`, { timeout: 120000 });
    const dls = r.data?.data?.downloads || [];
    if (!r.data?.status || dls.length === 0) throw new Error('Fallback API eken download links hamu una na');
    const hit = dls.find(d => cineNorm(d.quality) === cineNorm(quality)) || dls[index] || dls[0];
    if (!hit?.link) throw new Error('Fallback API eken link eka na');
    return { url: hit.link, quality: hit.quality };
}

async function cineFallbackEpisodeLink(showUrl, epList, selectedEpisode) {
    const info = await axios.get(`${CINE_FB_BASE}/api/v1/movie/cinesubz/tv/info?q=${encodeURIComponent(showUrl)}&api_key=${CINE_FB_KEY}`, { timeout: 120000 });
    const eps = info.data?.data?.episodes || [];
    if (!info.data?.status || eps.length === 0) throw new Error('Fallback API eken episodes hamu una na');
    let ep = eps.find(e => cineNorm(e.episode_name) && cineNorm(e.episode_name) === cineNorm(selectedEpisode.title));
    if (!ep) {
        const idx = (epList || []).findIndex(e => e.url === selectedEpisode.url);
        if (idx > -1 && eps[idx]) ep = eps[idx];
    }
    if (!ep) throw new Error('Fallback API eken e episode eka hamu una na');
    const dl = await axios.get(`${CINE_FB_BASE}/api/v1/movie/cinesubz/tv/dl?q=${encodeURIComponent(ep.episode_url)}&api_key=${CINE_FB_KEY}`, { timeout: 120000 });
    const arr = dl.data?.data || [];
    if (!dl.data?.status || arr.length === 0) throw new Error('Fallback API eken episode link eka na');
    const nonT = arr.filter(l => l.link && !l.link.includes('t.me') && !l.link.includes('telegram'));
    return { url: (nonT[0] || arr[0]).link };
}

// 📢 Channel ekata yawala WhatsApp eken reject kalada balanawa (ack error) - "yawwa" kiyala hariyata kiyanna
async function sendToChannelChecked(socket, jid, content) {
    const id = BaileysNS.generateMessageIDV2 ? BaileysNS.generateMessageIDV2(socket.user?.id) : BaileysNS.generateMessageID();
    let ackErr = null;
    const onAck = (node) => { if (node?.attrs?.error) ackErr = node.attrs.error; };
    try { socket.ws.on('TAG:' + id, onAck); } catch { /* ignore */ }
    const sent = await socket.sendMessage(jid, content, { messageId: id });
    await new Promise(r => setTimeout(r, 4000));
    try { socket.ws.off('TAG:' + id, onAck); } catch { /* ignore */ }
    if (ackErr) throw new Error(`WhatsApp channel post eka reject kala (code ${ackErr}). Bot number eka channel eke admin/owner nemei wage.`);
    return sent;
}

function formatMessage(title, content, footer) {
    return `╭━━━━━━━━━━━━━━━━━━━╮\n┃ ✨ *${title}* ✨\n╰━━━━━━━━━━━━━━━━━━━╯\n\n${content}\n\n━━━━━━━━━━━━━━━━━━━\n> 🎭 *${footer}* 🎭`;
}

function getSriLankaTimestamp() {
    return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss');
}

async function downloadContent(message) {
    if (!message) throw new Error('No message content');
    const buffer = await downloadContentFromMessage(message, 'buffer');
    return buffer;
}

async function streamToBuffer(stream) {
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(chunk);
    }
    return Buffer.concat(chunks);
}

// ==========================================
// 🆕 AUTO REPLY HANDLER
// ==========================================
async function setupAutoReply(socket, number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');

    // 🆕 Mulinma cache eka load karagannawa
    await loadAutoReplyCache(sanitizedNumber);

    // 🆕 Change Stream eken - web panel/.adauto eken rule ekak add/wenas/delete unama
    // cache eka background eke witharak refresh karanawa (message path eke DB query ekak nathuwa)
    if (autoReplyStreams.has(sanitizedNumber)) {
        try { autoReplyStreams.get(sanitizedNumber).close(); } catch (e) {}
    }
    try {
        const arStream = AutoReply.watch(
            [{ $match: { $or: [
                { 'fullDocument.number': sanitizedNumber },
                { operationType: 'delete' }
            ] } }],
            { fullDocument: 'updateLookup' }
        );
        arStream.on('change', () => {
            // Delete events walata fullDocument eka thiyenne nathi nisa, number eka check karanna
            // barii - ee nisa methanadi simple widihata cache eka refresh karala gannawa.
            loadAutoReplyCache(sanitizedNumber);
        });
        arStream.on('error', (err) => {
            console.error(`AutoReply change stream error for ${sanitizedNumber}:`, err.message);
        });
        autoReplyStreams.set(sanitizedNumber, arStream);
    } catch (streamErr) {
        console.error(`AutoReply change stream unavailable for ${sanitizedNumber}:`, streamErr.message);
    }

    socket.ev.on('messages.upsert', async ({ messages, type }) => {
        try {
            if (type !== 'notify') return;
            
            const msg = messages[0];
            if (!msg?.message) return;
            if (msg.key.remoteJid === 'status@broadcast') return;
            if (msg.key.fromMe) return;
            
            let text = '';
            if (msg.message.conversation) {
                text = msg.message.conversation.trim();
            } else if (msg.message.extendedTextMessage?.text) {
                text = msg.message.extendedTextMessage.text.trim();
            } else {
                return;
            }
            
            if (!text) return;
            if (text.startsWith('.')) return;
            
            const lowerText = text.toLowerCase();
            // 🆕 DB query ekak nathuwa - memory cache ekenma check karanawa (super fast, delay nathuwa)
            const found = autoReplyCache.get(sanitizedNumber)?.get(lowerText);
            
            if (found) {
                // 🆕 Image ekak thiyenawa nam image + caption widihata, nathnam plain text widihata yawanawa
                if (found.image) {
                    await socket.sendMessage(msg.key.remoteJid, {
                        image: { url: found.image },
                        caption: found.reply || ''
                    }, { quoted: msg });
                } else {
                    await socket.sendMessage(msg.key.remoteJid, {
                        text: found.reply
                    }, { quoted: msg });
                }
                
                console.log(`💬 Auto-reply: "${lowerText}" → ${msg.key.remoteJid.split('@')[0]}`);
            }
            
        } catch (err) {
            console.error('AutoReply error:', err.message);
        }
    });
    
    console.log('✅ Auto-reply handler ready');
}

async function setupCommandHandlers(socket, number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    let sessionConfig = await loadUserConfig(sanitizedNumber);
    activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });

    // 🔘 Button mode engine (BUTTON_MODE=true nam numbered lists buttons wenawa)
    // 🔒 Button mode: master (94784224161) + .add eken subscription dunna numbers walata witharai
    const buttonAllowed = () => {
        if ((config.OWNER_NUMBERS || []).includes(sanitizedNumber)) return true;
        const exp = sessionConfig.LICENSE_EXPIRY ? new Date(sessionConfig.LICENSE_EXPIRY).getTime() : 0;
        return exp > Date.now();
    };
    const buttonLockedMsg = `🔒 *Payment Required!*\n\n_Button mode (mulu bot ekama) paid feature ekak. *Pay karala subscription ekak* ganna._\n\n💳 *Pay karala ganna owner ta contact karanna:*\n👑 wa.me/${(config.OWNER_NUMBERS || [])[0] || ''}`;
    installButtonMode(socket, {
        isOn: () => sessionConfig.BUTTON_MODE === 'true' && buttonAllowed(),
        prefix: () => sessionConfig.PREFIX || config.PREFIX || '.'
    });

    // 🆕 Web panel eken / wenath tenakin DB eke config eka wenas kalath, MongoDB Change Stream eken
    // (polling nathuwa) real-time widihata bot ekatama apply karanawa. Meka DB ekata continuous load
    // ekak dammema (poll karanne nathuwa) - change ekak welavakama witharak fire wenne.
    // ⚠️ IMPORTANT: pena stream/interval ekak thiyenawa nam mulinma clear karanawa - reconnect wena hama welawakama
    // alut ekak stack wela bot eka slow wena eka nawaththanna
    clearNumberIntervals(sanitizedNumber);

    // 🔄 Config sync: minutes 5kata ekaparak DB eke config eka balala, wenas unoth witharai bot ekata apply karanawa.
    //  - number ekakata timer ekak witharai (clearNumberIntervals eken kalin eka ain karanawa) => stack wenne na
    //  - kalin run eka iwara wenakan next run eka patan ganne na (syncBusy) => overlap / loop nathi
    //  - .set / .add eken wenas kala hama welawema ikmanata apply wenawa; meka DB eke wenas kirim walata
    const CONFIG_SYNC_MS = 5 * 60 * 1000;
    const stableCfg = (o) => JSON.stringify(o || {}, Object.keys(o || {}).sort());
    let syncBusy = false;
    const configSyncTimer = setInterval(async () => {
        if (syncBusy) return;
        syncBusy = true;
        try {
            const fresh = await loadUserConfig(sanitizedNumber);
            if (stableCfg(fresh) !== stableCfg(sessionConfig)) {
                sessionConfig = fresh;
                activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });
                console.log(`🔄 Config synced (5 min) for ${sanitizedNumber}`);
            }
        } catch (syncErr) {
            // DB temporary issue - next tick ekedi ayeth try karanawa
        } finally {
            syncBusy = false;
        }
    }, CONFIG_SYNC_MS);
    if (configSyncTimer.unref) configSyncTimer.unref();
    configSyncIntervals.set(sanitizedNumber, configSyncTimer);

    // Socket eka close/logout unama stream/interval eka clear karanawa (memory leak walakwanna)
    socket.ev.on('connection.update', (update) => {
        if (update.connection === 'close') {
            clearNumberIntervals(sanitizedNumber);
        }
    });

    socket.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message) return;

        // 🆕 STATUS UPDATES: Auto View + Auto Like handle karanawa methanin
        if (msg.key.remoteJid === 'status@broadcast') {
            try {
                if (sessionConfig.STATUS_VIEW === 'true') {
                    await socket.readMessages([msg.key]);
                }
                if (sessionConfig.AUTO_LIKE === 'true') {
                    const likeEmojis = ['❤️', '🔥', '😍', '👍', '😂', '💕', '🎉', '✨'];
                    const emoji = likeEmojis[Math.floor(Math.random() * likeEmojis.length)];
                    await socket.sendMessage('status@broadcast', {
                        react: { text: emoji, key: msg.key }
                    }, { statusJidList: [msg.key.participant, jidNormalizedUser(socket.user.id)] });
                }
            } catch (statusErr) {
                console.error('Status auto view/like error:', statusErr.message);
            }
            return;
        }

        // 🆕 ANTI-DELETE: delete kale kawuda / eka yawwe kawuda kiyala penawa
        if (msg.message.protocolMessage && (msg.message.protocolMessage.type === 0 || msg.message.protocolMessage.type === 'REVOKE')) {
            try {
                if (sessionConfig.ANTI_DELETE === 'true' && !msg.key.fromMe) {
                    const chatJid = msg.key.remoteJid;
                    const deletedId = msg.message.protocolMessage.key?.id;
                    const ck = `${chatJid}:${deletedId}`;
                    const raw = rawCache.get(ck);
                    const cachedTxt = messageCache.get(ck);
                    if (raw || cachedTxt) {
                        const numOf = (j) => String(j || '').split('@')[0].split(':')[0];
                        const pn = (j, alt) => (alt && String(alt).endsWith('@s.whatsapp.net')) ? numOf(alt) : numOf(j);
                        const isGrp = chatJid.endsWith('@g.us');

                        const sentBy = raw
                            ? (isGrp ? pn(raw.participant, raw.participantAlt) : pn(raw.remoteJid, raw.remoteJidAlt))
                            : (cachedTxt?.senderNumber || 'unknown');
                        const deletedBy = isGrp
                            ? pn(msg.key.participant, msg.key.participantAlt)
                            : pn(chatJid, msg.key.remoteJidAlt);

                        let chatName = 'Private Chat';
                        if (isGrp) {
                            try { chatName = (await socket.groupMetadata(chatJid)).subject || 'Group'; } catch { chatName = 'Group'; }
                        }

                        const inner = raw ? unwrap(raw.msg.message) : {};
                        const typeKey = Object.keys(inner).find(k => k !== 'messageContextInfo') || 'text';
                        const typeName = typeKey.replace('Message', '').replace('extendedText', 'text').replace('conversation', 'text');
                        const body = cachedTxt?.text
                            || inner.conversation || inner.extendedTextMessage?.text
                            || inner.imageMessage?.caption || inner.videoMessage?.caption || '';

                        const ownerJid = jidNormalizedUser(socket.user.id);
                        await socket.sendMessage(ownerJid, {
                            text:
`╭━━━〔 🗑️ *𝗔𝗡𝗧𝗜 𝗗𝗘𝗟𝗘𝗧𝗘* 🗑️ 〕━━━╮
┃
┃ 🚫 *Deleted By :* wa.me/${deletedBy}
┃ 📤 *Sent By    :* wa.me/${sentBy}${raw?.pushName ? ` (${raw.pushName})` : ''}
┃ 💬 *Chat       :* ${chatName}
┃ 📎 *Type       :* ${typeName}
┃ 🕒 *Time       :* ${getSriLankaTimestamp()}
┃
╰━━━━━━━━━━━━━━━━━━━━╯${body ? `\n\n📩 *Message:*\n${body}` : ''}

> 🎭 *SHAGGY XMD* 🎭`
                        });

                        // media nam original eka forward karanawa
                        if (raw && typeName !== 'text' && typeName !== 'protocol') {
                            try { await socket.sendMessage(ownerJid, { forward: raw.msg }); }
                            catch (fwdErr) { console.error('AntiDelete forward failed:', fwdErr.message); }
                        }
                        messageCache.delete(ck);
                        rawCache.delete(ck);
                    }
                }
            } catch (adErr) {
                console.error('AntiDelete error:', adErr.message);
            }
            return;
        }

        // 🆕 anti-delete: hama message ekakma (media ekath) cache karanawa
        try {
            if (!msg.key.fromMe && msg.message && !msg.message.protocolMessage) cacheRaw(msg);
        } catch { /* ignore */ }

        let text = '';
        if (msg.message.conversation) {
            text = msg.message.conversation.trim();
        } else if (msg.message.extendedTextMessage?.text) {
            text = msg.message.extendedTextMessage.text.trim();
        } else if (msg.message.buttonsResponseMessage) {
            text = msg.message.buttonsResponseMessage.selectedButtonId;
        } else {
            return;
        }

        const userJid = jidNormalizedUser(socket.user.id);
        const from = msg.key.remoteJid;
        const sender = from;
        const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net' || socket.user.id) : (msg.key.participant || msg.key.remoteJid);
        const senderNumber = (nowsender || '').split('@')[0];

        // 🆕 message eka cache karanawa - anti-delete walata one wenne methanin
        try {
            cacheMessage(from, msg.key.id, { text, senderNumber });
        } catch (cacheErr) { /* ignore */ }

        const developers = `${config.OWNER_NUMBERS}`;
        const botNumber = socket.user.id.split(':')[0];
        const isbot = botNumber.includes(senderNumber);
        const isOwner = isbot ? isbot : developers.includes(senderNumber);
        const isGroup = from.endsWith("@g.us");
        const isCmd = text.startsWith(sessionConfig.PREFIX || '!');

        // 🆕 SUBSCRIPTION / LICENSE CHECK - master owner ge number eka witharai free.
        // wenath number ekakata license ekak nathnam/ivara wela nam, commands process karanne nah.
        const isMasterBotNumber = (config.OWNER_NUMBERS || []).includes(sanitizedNumber);
        if (!isMasterBotNumber) {
            const expiryStr = sessionConfig.LICENSE_EXPIRY;
            const expiryTime = expiryStr ? new Date(expiryStr).getTime() : 0;
            const isLicensed = expiryTime > Date.now();
            if (!isLicensed && isCmd) {
                await socket.sendMessage(sender, {
                    text: `⛔ *Payment Required!*\n\n_Mee bot eka paid bot ekak. Use karanna nam *pay karala subscription ekak* ganna one (subscription nathi / ivara wela)._\n\n💳 *Pay karala ganna owner ta contact karanna:*\n👑 wa.me/${(config.OWNER_NUMBERS || [])[0] || ''}`
                }, { quoted: msg });
                return;
            }
        }

        if (!isOwner && sessionConfig.MODE === 'private') return;
        if (!isOwner && isGroup && sessionConfig.MODE === 'inbox') return;
        if (!isOwner && !isGroup && sessionConfig.MODE === 'groups') return;

        // 🆕 AUTO SEEN: hama message ekakma auto widihata seen karanawa (command da nemei kiyala check karanne nathuwa)
        if (sessionConfig.ALWAYS_MSG_SEEN === 'true' && !msg.key.fromMe) {
            try {
                await socket.readMessages([msg.key]);
            } catch (seenErr) {
                // ignore
            }
        }

        if (isCmd && sessionConfig.READ_CMD === 'true') {
            try {
                await socket.readMessages([msg.key]);
            } catch (error) {

            }
        }

        if (!isCmd) return;
        const parts = text.slice((sessionConfig.PREFIX || '!').length).trim().split(/\s+/);
        const command = parts[0].toLowerCase();
        const args = parts.slice(1);

        const groupMetadata = isGroup ? await socket.groupMetadata(msg.key.remoteJid) : {};
        const participants = groupMetadata.participants || [];
        const groupAdmins = participants.filter((p) => p.admin).map((p) => p.id);
        const isBotAdmins = groupAdmins.includes(socket.user.id);
        const isAdmins = groupAdmins.includes(sender);

        const reply = async (text, options = {}) => {
            await socket.sendMessage(msg.key.remoteJid, { text, ...options }, { quoted: msg });
        };

        try {
            switch (command) {
                // ✅ ඔයාගේ cases ටික මෙතනට එනවා
            case 'csong': {
                // .csong <channel link>, <song name>  -> song eka channel ekata post karanawa (owner witharai)
                if (!isOwner) {
                    return await socket.sendMessage(sender, { text: '❌ *Owner witharai meka use karanna puluwan.*' }, { quoted: msg });
                }
                const csRaw = args.join(' ');
                const csComma = csRaw.indexOf(',');
                if (csComma === -1 || !csRaw.slice(0, csComma).trim() || !csRaw.slice(csComma + 1).trim()) {
                    return await socket.sendMessage(sender, {
                        text: `❌ *Use karana widiya:*\n\n\`${sessionConfig.PREFIX || '.'}csong <channel link>, <song name>\`\n\n*Example:*\n${sessionConfig.PREFIX || '.'}csong https://whatsapp.com/channel/0029VaXXXXXXX, Lelena\n\n_Bot number eka channel eke admin/owner wela inna one._`
                    }, { quoted: msg });
                }
                const csLink = csRaw.slice(0, csComma).trim();
                const csQuery = csRaw.slice(csComma + 1).trim();

                try {
                    await socket.sendMessage(sender, { react: { text: '🔎', key: msg.key } });

                    // 1) channel eka hoyaganna
                    let channelJid = null, channelName = 'Channel';
                    if (/@newsletter$/.test(csLink)) {
                        channelJid = csLink;
                    } else {
                        const codeM = csLink.match(/channel\/([A-Za-z0-9_-]+)/);
                        if (!codeM) throw new Error('Channel link eka waradi (whatsapp.com/channel/... link ekak denna)');
                        const meta = await socket.newsletterMetadata('invite', codeM[1]);
                        if (!meta?.id) throw new Error('Channel eka hoyaganna baha');
                        channelJid = meta.id;
                        channelName = meta.name || meta.thread_metadata?.name?.text || 'Channel';
                        const roleRaw = String(meta.viewer_metadata?.role || meta.role || meta.viewer?.role || '').toUpperCase();
                        if (roleRaw && !['OWNER', 'ADMIN'].includes(roleRaw)) {
                            throw new Error(`Bot number eka meka channel eke ${roleRaw} kenek. Post karanna nam *admin / owner* wenna one (channel eke Admins wala bot number eka add karanna).`);
                        }
                    }

                    // 2) song eka hoyaganna
                    const csRes = await yts(csQuery);
                    const song = csRes.videos?.[0];
                    if (!song) throw new Error(`"${csQuery}" kiyana song eka hamu una na`);

                    await socket.sendMessage(sender, { text: `⏳ *${song.title}*\n_Download karala ${channelName} ekata post karanawa..._` }, { quoted: msg });

                    // 3) mp3 download link eka
                    const csApi = `${config.API_MAIN_URL}/api/ytmp3?url=https://youtu.be/${song.videoId}&api_key=${config.API_KEY}`;
                    const csDl = await axios.get(csApi, { timeout: 120000 });
                    if (csDl.data?.status !== 'success') throw new Error(csDl.data?.message || 'Song API error');
                    const mp3Url = csDl.data.data.download_url;
                    const songTitle = csDl.data.data.title || song.title;
                    const thumb = csDl.data.data.thumbnail || song.thumbnail;

                    const audioBuf = Buffer.from((await axios.get(mp3Url, { responseType: 'arraybuffer', timeout: 120000, maxContentLength: Infinity })).data);
                    if (audioBuf.length < 50 * 1024) throw new Error('Song file eka waradi (godak podi)');

                    // 4) lassana description ekak ekka channel ekata post
                    const caption =
`╭━━━〔 🎧 *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗠𝗨𝗦𝗜𝗖* 🎧 〕━━━╮
┃
┃ 🎵 *Title*     : _${songTitle}_
┃ 🎤 *Artist*    : _${song.author?.name || 'N/A'}_
┃ ⏱️ *Duration*  : _${song.timestamp || 'N/A'}_
┃ 👀 *Views*     : _${song.views?.toLocaleString() || 'N/A'}_
┃ 📅 *Published* : _${song.ago || 'N/A'}_
┃
╰━━━━━━━━━━━━━━━━━━━━╯

🎶 _Headphones dala rasa wind wenna_ 🎧
👇 *Song eka pahala thiyenawa* 👇

❤️ *React karanna* | 🔁 *Share karanna*

> 🎭 *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗* 🎭`;

                    await sendToChannelChecked(socket, channelJid, { image: { url: thumb }, caption });
                    await sendToChannelChecked(socket, channelJid, { audio: audioBuf, mimetype: 'audio/mpeg', ptt: false });

                    await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
                    await socket.sendMessage(sender, {
                        text: `✅ *Channel ekata post kala!*\n\n🎵 ${songTitle}\n📢 ${channelName}`
                    }, { quoted: msg });
                } catch (csErr) {
                    console.error('[csong] error:', csErr.message);
                    await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } }).catch(() => {});
                    let hint = '';
                    if (/forbidden|not.?authorized|401|403|admin|permission/i.test(csErr.message)) hint = '\n\n_Bot number eka meka channel eke admin/owner nemei wage._';
                    await socket.sendMessage(sender, { text: `❌ *CSONG Error:* ${csErr.message}${hint}` }, { quoted: msg });
                }
                break;
            }

            case 'song':
    if (!args.length) {
        await socket.sendMessage(sender, {
            text: '❌ ERROR\n\n*Need YouTube URL or Song Title*'
        }, { quoted: msg });
        break;
    }

    const songQuery = args.join(' ');
    await socket.sendMessage(sender, { text: '🔍 Searching song...' });

    try {
        let data;
        if (songQuery.match(/(youtube\.com|youtu\.be)/)) {
            const match = songQuery.match(/(?:v=|\/)([0-9A-Za-z_-]{11})/);
            const videoId = match ? match[1] : null;

            if (!videoId) throw new Error('Invalid YouTube URL');

            const result = await yts({ videoId });
            data = result;
        } else {
            const result = await yts(songQuery);

            if (!result.videos || result.videos.length === 0) {
                await socket.sendMessage(sender, {
                    text: '❌ NO RESULTS\n\n*No results found for your query*'
                }, { quoted: msg });
                break;
            }

            data = result.videos[0];
        }

        if (!data) throw new Error('No results');

        const videoId = data.videoId;
        const desc = ` *ᴛɪᴛʟᴇ* : _${data.title || 'N/A'}_     

* ⏱️ 𝗗ᴜʀᴀᴛɪᴏɴ* ➟ _${data.timestamp || 'N/A'}_
* 👀 𝗩ɪᴇᴡꜱ* ➟ _${data.views?.toLocaleString() || 'N/A'}_
* 📅 𝗣ᴜʙʟɪꜱʜᴇᴅ* ➟ _${data.ago || 'N/A'}_
* 🎤 𝗖ʜᴀɴɴᴇʟ* ➟ _${data.author?.name || 'N/A'}_
*🔢 𝗥ᴇᴘʟʏ ᴡɪᴛʜ ᴀ 𝗡ᴜᴍʙᴇʀ 👇*

*01 ᴅᴏᴡɴʟᴏᴀᴅ ᴀᴜᴅɪᴏ 🌐*
*02 ᴅᴏᴡɴʟᴏᴀᴅ ᴅᴏᴄᴜᴍᴇɴᴛ 🌐*
`;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: data.thumbnail },
            caption: desc
        }, { quoted: msg });
        const listener = async (update) => {
            const mek = update.messages[0];
            if (!mek?.message) return;
            const ctx = mek.message.extendedTextMessage?.contextInfo;
            if (!ctx || ctx.stanzaId !== sentMsg.key.id) return;
            const text =
                mek.message.conversation ||
                mek.message.extendedTextMessage?.text;

            if (!['1', '2'].includes(text)) return;
            socket.ev.off('messages.upsert', listener);

            await socket.sendMessage(sender, { react: { text: '⬇️', key: mek.key } });

            try {
                 const apiUrl = `${config.API_MAIN_URL}/api/ytmp3?url=https://youtu.be/${videoId}&api_key=${config.API_KEY}`;
                const res = await axios.get(apiUrl, { timeout: 20000 });

                if (res.data.status !== 'success') {
                    throw new Error(res.data.message || 'API Error');
                }
                const downloadLink = res.data.data.download_url;
                const songTitle = res.data.data.title || data.title;
                const thumbnail = res.data.data.thumbnail || data.thumbnail;
                await socket.sendMessage(sender, { react: { text: '⬆️', key: mek.key } });
                const fileName = songTitle.replace(/[^a-zA-Z0-9]/g, '_');
                if (text === '1') {
                    await socket.sendMessage(sender, {
                        audio: { url: downloadLink },
                        mimetype: 'audio/mpeg'
                    }, { quoted: mek });
                } else if (text === '2') {
                    await socket.sendMessage(sender, {
                        document: { url: downloadLink },
                        mimetype: 'audio/mpeg',
                        fileName: `${fileName}.mp3`,
                        caption: songTitle
                    }, { quoted: mek });
                }

                await socket.sendMessage(sender, { react: { text: '✅', key: mek.key } });

            } catch (err) {
                await socket.sendMessage(sender, {
                    text: '❌ DOWNLOAD ERROR\n\n' + err.message
                }, { quoted: mek });

                await socket.sendMessage(sender, { react: { text: '❌', key: mek.key } });
            }
        };

        socket.ev.on('messages.upsert', listener);
        setTimeout(() => {
            socket.ev.off('messages.upsert', listener);
        }, 300000);

    } catch (err) {
        await socket.sendMessage(sender, {
            text: '❌ ERROR\n\n' + err.message
        }, { quoted: msg });
    }

    break;  
                 case 'tiktok':
    if (!args.length || !args.join(' ').startsWith('https://')) {
        await socket.sendMessage(sender, {
            image: { url: config.ERROR },
            caption: `❌ ERROR

Please provide a valid TikTok URL!

📋 Example: .tiktok  https://www.tiktok.com/@user/video/xyz`
        });
        break;
    }

    await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

    let tiktokTimeout;

    try {
        const tiktokUrl = args.join(' ');
        const response = await axios.get(`${config.API_MAIN_URL}/tiktok/download?url=${encodeURIComponent(tiktokUrl)}&api_key=${config.API_KEY}`);
        const tiktokData = response.data.result;

        if (!response.data.status || !tiktokData) {
            await socket.sendMessage(sender, {
                image: { url: config.ERROR },
                caption: `❌ ERROR

Failed to fetch TikTok video! Please try again later.`
            });
            break;
        }

        const captionMessage = `☘️ *TIKTOK DOWNLOADER*

📝 Title: ${tiktokData.title || 'TikTok Video'}
👤 Author: ${tiktokData.author?.nickname || 'Unknown'}
❤️ Likes: ${tiktokData.digg_count?.toLocaleString() || 'N/A'}
👀 Views: ${tiktokData.play_count?.toLocaleString() || 'N/A'}
💬 Comments: ${tiktokData.comment_count?.toLocaleString() || 'N/A'}
⏱️ Duration: ${tiktokData.duration || 'N/A'} seconds

⬇️ DOWNLOAD OPTIONS

🔢 Reply with a number:

*1 ║❯❯ No Watermark ☊*
*2 ║❯❯ With Watermark ☊*
*3 ║❯❯ Audio Only ☊*`;

        const sentMessage = await socket.sendMessage(sender, {
            image: { url: tiktokData.cover || config.SITHIJA_IMAGE_PATH },
            caption: captionMessage
        }, { quoted: msg });

        const messageID = sentMessage.key.id;

        const handleTikTokSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const userResponse = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                if (tiktokTimeout) clearTimeout(tiktokTimeout);

                await socket.sendMessage(sender, { react: { text: '⬇️', key: replyMek.key } });

                const downloadLinks = tiktokData.downloads;
                let mediaMessage;

                try {
                    switch (userResponse) {
                        case '1':
                            mediaMessage = {
                                video: { url: downloadLinks.no_watermark },
                                mimetype: 'video/mp4',
                                caption: `✅ TIKTOK VIDEO

No Watermark Video
📝 ${tiktokData.title}`
                            };
                            break;
                        case '2':
                            mediaMessage = {
                                video: { url: downloadLinks.watermark },
                                mimetype: 'video/mp4',
                                caption: `✅ TIKTOK VIDEO

With Watermark Video
📝 ${tiktokData.title}`
                            };
                            break;
                        case '3':
                            mediaMessage = {
                                audio: { url: downloadLinks.audio },
                                mimetype: 'audio/mpeg',
                                caption: `✅ TIKTOK AUDIO

Audio Only
📝 ${tiktokData.title}`
                            };
                            break;

                        default:
                            await socket.sendMessage(sender, {
                                image: { url: config.ERROR },
                                caption: `❌ INVALID SELECTION

Please reply with 1, 2, 3, or 4.`
                            });
                            return;
                    }

                    await socket.sendMessage(sender, mediaMessage, { quoted: replyMek });
                    await socket.sendMessage(sender, { react: { text: '✅', key: replyMek.key } });

                } catch (sendError) {
                    console.error('TikTok send error:', sendError);
                    await socket.sendMessage(sender, {
                        image: { url: config.ERROR },
                        caption: `❌ ERROR

Failed to send: ${sendError.message}`
                    }, { quoted: replyMek });
                } finally {
                    socket.ev.off('messages.upsert', handleTikTokSelection);
                }
            }
        };

        socket.ev.on('messages.upsert', handleTikTokSelection);

        tiktokTimeout = setTimeout(() => {
            socket.ev.off('messages.upsert', handleTikTokSelection);
            console.log('TikTok selection timeout - cleaned up');
        }, 120000);

    } catch (error) {
        console.error('TikTok download error:', error);
        await socket.sendMessage(sender, {
            image: { url: config.ERROR },
            caption: `❌ ERROR

Failed to process TikTok request: ${error.message}`
        });
    }
    break;
case 'cinesubz':
    if (!args.length) {
        await socket.sendMessage(sender, {
            image:  { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ හෝ TV series එකේ නම ලබාදෙන්න! උදා: .cinesubz batman*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const cinezubQuerytv = args.join(' ');
    await socket.sendMessage(sender, { text: '📽️ 𝙎𝙚𝙖𝙧𝙘𝙝𝙞𝙣𝙜 𝙤𝙣 𝘾𝙞𝙣𝙚𝙨𝙪𝙗𝙯...' });

    try {
        const searchResponse = await axios.get(`https://apis.laksidu.site/cinesubz/search?query=${encodeURIComponent(cinezubQuerytv)}&api_key=lakiyaofc2`);
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.results || searchData.results.length === 0) {
            await socket.sendMessage(sender, {
                image:  { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*cinesubz හි චිත්‍රපට හමුවෙන්නේ නැත! 😞*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const cinezubResults = searchData.results.slice(0, 25);
        let listText = `☘️ *𝗠𝗢𝗩𝗜𝗘 : _𝗦𝗘𝗔𝗥𝗖𝗛 𝗥𝗘𝗦𝗨𝗟𝗧𝗦_* 🔍
╭──────●➤
🔎 *𝗤𝘂𝗲𝗿𝘆 ➟* _${cinezubQuerytv}_
📊 *Status ➟* _Results Found_
╰──────────●➤
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤
💡 *𝗥ᴇᴘʟʏ ᴡɪᴛʜ ᴀ 𝗡ᴜᴍʙᴇʀ 𝘁ᴏ 𝗦ᴇʟᴇᴄ𝘛*
*╭──────●➤*\n\n`;

        cinezubResults.forEach((item, index) => {
            const type = item.link.includes('/tvshows/') ? '📺 TV Series' : '🎬 Movie';
            listText += `*🍟${index + 1} ║❯❯ ${type} | ${item.title}*\n`;
        });

        listText += `╰──────────●➤\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: config.BOT_IMAGE},
            caption: listText
        }, { quoted: msg });

        const messageID = sentMsg.key.id;

        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= cinezubResults.length) {
                    await socket.sendMessage(sender, {
                        image:  { url: pickBotImage(sessionConfig) },
                        caption: formatMessage(
                            '❌ INVALID SELECTION',
                            `*වැරදි අංකයක්! 1-${cinezubResults.length} අතර තෝරන්න! 😕*`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                    return;
                }

                const selectedItem = cinezubResults[choice];
                const isTvShow = selectedItem.link.includes('/tvshows/');

                if (isTvShow) {
                    await socket.sendMessage(sender, { 
                        text: '📺 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙏𝙑 𝙨𝙚𝙧𝙞𝙚𝙨 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' 
                    }, { quoted: replyMek });

                    try {
                        const tvShowResponse = await axios.get(`https://apis.laksidu.site/cinesubz/tvshow?url=${encodeURIComponent(selectedItem.link)}&api_key=lakiyaofc2`);
                        const tvShowData = tvShowResponse.data;

                        if (!tvShowData.status || !tvShowData.data) {
                            throw new Error('Failed to fetch TV show details');
                        }

                        const tvInfo = tvShowData.data;

                        // 🟢 NEW STRUCTURE - Extract data correctly
                        const rating = tvInfo.rating?.score || 'N/A';
                        const totalEpisodes = tvInfo.episodes?.total || 'N/A';
                        const episodesList = tvInfo.episodes?.list || [];

                        // Group episodes by season (extract season from episode title or URL)
                        const seasonsMap = {};
                        episodesList.forEach(ep => {
                            let seasonNum = '1';
                            // Try to extract season from episode number or title
                            const seasonMatch = ep.number?.match(/^(\d+)/);
                            if (seasonMatch) {
                                seasonNum = seasonMatch[1];
                            }
                            if (!seasonsMap[seasonNum]) {
                                seasonsMap[seasonNum] = [];
                            }
                            seasonsMap[seasonNum].push(ep);
                        });

                        const seasonsArray = Object.keys(seasonsMap).map(season => ({
                            season: parseInt(season),
                            total_episodes: seasonsMap[season].length,
                            episodes: seasonsMap[season].map(ep => ({
                                episode: ep.number || '1',
                                title: ep.title || 'Episode',
                                url: ep.url || ''
                            }))
                        }));

                        const totalSeasons = seasonsArray.length;

                        let tvDetailsText = 
    `☘️ *𝗧ɪᴛʟᴇ ➟* _${tvInfo.title || 'N/A'}_
▫️🥇 *𝗜𝗺𝗱𝗯 𝗥ᴀᴛɪɴɢ ➟*  _${rating}_
▫️📅 *𝗥ᴇʟᴇᴀꜱᴇ 𝗬ᴇᴀʀ ➟*_${tvInfo.year || 'N/A'}_
▫️📀 *𝗦ᴇᴀꜱᴏɴꜱ ➟* _${totalSeasons} Total_
▫️📊 *𝗘ᴘɪꜱᴏᴅᴇꜱ ➟* _${totalEpisodes} Total_
*➟➟➟➟➟➟➟➟➟➟*
📖 *𝗦𝗧𝗢𝗥𝗬*_${tvInfo.description?.substring(0, 30) || 'No description available.'}..._`;

                        await socket.sendMessage(sender, {
                            image: { url: tvInfo.poster || sessionConfig.LAKIYA_IMAGE_PATH || config.LAKIYA_IMAGE_PATH },
                            caption: tvDetailsText
                        }, { quoted: replyMek });

                        let seasonsText = 
    `☘️ *𝗧𝗩-𝗦𝗘𝗥𝗜𝗘𝗦 : _𝗦𝗘𝗔𝗦𝗢𝗡 𝗦𝗘𝗟𝗘𝗖𝗧𝗜𝗢𝗡_* 📺
*➟➟➟➟➟➟➟➟➟➟*
⬇️🍀 *𝗦𝗘𝗟𝗘𝗖𝗧 𝗬𝗢𝗨𝗥 𝗦𝗘𝗔𝗦𝗢𝗡*
*➟➟➟➟➟➟➟➟➟➟*
💡 *𝗥ᴇᴘʟʏ ᴡɪᴛʜ ᴀ 𝗡ᴜᴍʙᴇʀ 𝘁ᴏ 𝗦ᴇʟᴇᴄ𝘛*
*➟➟➟➟➟➟➟➟➟➟*\n\n`;

                        seasonsArray.forEach((season, idx) => {
                            seasonsText += `🍀 *${idx + 1} ┃》📀 Season ${season.season} (${season.total_episodes} episodes)*\n`;
                        });

                        seasonsText += `\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

                        const seasonMsg = await socket.sendMessage(sender, {
                            text: seasonsText
                        }, { quoted: replyMek });

                        const seasonMsgID = seasonMsg.key.id;

                        const handleSeasonSelect = async ({ messages: seasonMessages }) => {
                            const seasonMek = seasonMessages[0];
                            if (!seasonMek?.message) return;

                            const seasonChoice = seasonMek.message.conversation || seasonMek.message.extendedTextMessage?.text;
                            const isReplyToSeasonMsg = seasonMek.message.extendedTextMessage?.contextInfo?.stanzaId === seasonMsgID;

                            if (isReplyToSeasonMsg && sender === seasonMek.key.remoteJid) {
                                const seasonNum = parseInt(seasonChoice) - 1;

                                if (isNaN(seasonNum) || seasonNum < 0 || seasonNum >= seasonsArray.length) {
                                    await socket.sendMessage(sender, {
                                        image:  { url: pickBotImage(sessionConfig) },
                                        caption: formatMessage(
                                            '❌ INVALID SELECTION',
                                            `*වැරදි අංකයක්! 1-${seasonsArray.length} අතර තෝරන්න!*`,
                                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                        )
                                    }, { quoted: seasonMek });
                                    return;
                                }

                                const selectedSeason = seasonsArray[seasonNum];

                                let episodesText =
    `☘️ *𝗧𝗩-𝗦𝗘𝗥𝗜𝗘𝗦 : _𝗘𝗣𝗜𝗦𝗢𝗗𝗘 𝗦𝗘𝗟𝗘𝗖𝗧𝗜𝗢𝗡_* 📺
╭──────●➤
☘️ *𝗧ɪᴛʟᴇ ➟* _${tvInfo.title || 'N/A'}_
📀 *𝗦ᴇᴀꜱᴏɴ ➟* _Season ${selectedSeason.season}_
📊 *𝗧ᴏᴛᴀʟ ➟* _${selectedSeason.total_episodes} Episodes_
╰──────────●➤
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤\n\n`;

                                selectedSeason.episodes.forEach((ep, idx) => {
                                    episodesText += `*⭐${idx + 1} ║❯❯ 📺 Episode ${ep.episode}: ${ep.title}*\n`;
                                });

                                episodesText += `\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

                                const episodeMsg = await socket.sendMessage(sender, {
                                    text: episodesText
                                }, { quoted: seasonMek });

                                const episodeMsgID = episodeMsg.key.id;

                                const handleEpisodeSelect = async ({ messages: episodeMessages }) => {
                                    const episodeMek = episodeMessages[0];
                                    if (!episodeMek?.message) return;

                                    const episodeChoice = episodeMek.message.conversation || episodeMek.message.extendedTextMessage?.text;
                                    const isReplyToEpisodeMsg = episodeMek.message.extendedTextMessage?.contextInfo?.stanzaId === episodeMsgID;

                                    if (isReplyToEpisodeMsg && sender === episodeMek.key.remoteJid) {
                                        const choiceNum = parseInt(episodeChoice);

                                        if (isNaN(choiceNum) || choiceNum < 1 || choiceNum > selectedSeason.episodes.length) {
                                            await socket.sendMessage(sender, {
                                                image:  { url: pickBotImage(sessionConfig) },
                                                caption: formatMessage(
                                                    '❌ INVALID SELECTION',
                                                    `*වැරදි අංකයක්! 1-${selectedSeason.episodes.length} අතර තෝරන්න!*`,
                                                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                )
                                            }, { quoted: episodeMek });
                                            return;
                                        }

                                        const selectedEpisode = selectedSeason.episodes[choiceNum - 1];

                                        await socket.sendMessage(sender, { 
                                            text: `📥 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠𝙨 𝙛𝙤𝙧 S${selectedSeason.season}E${selectedEpisode.episode}...` 
                                        }, { quoted: episodeMek });

                                        try {
                                            // 🟢 NEW: Episode API URL
                                            const episodeResponse = await axios.get(`https://apis.laksidu.site/api/episode?url=${encodeURIComponent(selectedEpisode.url)}&api_key=lakiyaofc2`);
                                            const episodeData = episodeResponse.data;

                                            if (!episodeData.status || !episodeData.data?.download_links?.length) {
                                                throw new Error('Failed to get episode download links');
                                            }

                                            const episodeDownloadLinks = episodeData.data.download_links;

                                            let qualityText = 
    `☘️ *𝗧𝗩-𝗦𝗘𝗥𝗜𝗘𝗦 : _𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦_* 📺
╭──────●➤
🎬 *𝗧ɪᴛʟᴇ ➟* _${tvInfo.title || 'N/A'}_
📀 *𝗦ᴇᴀꜱᴏɴ ➟* _Season ${selectedSeason.season}_
📺 *𝗘ᴘɪꜱᴏᴅᴇ ➟* _${selectedEpisode.episode} : ${selectedEpisode.title}_
╰──────────●➤
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤\n\n`;

                                            episodeDownloadLinks.forEach((link, idx) => {
                                                const quality = link.meta || link.type || `Quality ${idx + 1}`;
                                                qualityText += `🔥 *${idx + 1} ║❯❯ 📥 ${quality}*\n`;
                                            });

                                            qualityText += `\n${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

                                            const qualityMsg = await socket.sendMessage(sender, {
                                                text: qualityText
                                            }, { quoted: episodeMek });

                                            const qualityMsgID = qualityMsg.key.id;

                                            const handleQualitySelect = async ({ messages: qualityMessages }) => {
                                                const qualityMek = qualityMessages[0];
                                                if (!qualityMek?.message) return;

                                                const qualityChoice = qualityMek.message.conversation || qualityMek.message.extendedTextMessage?.text;
                                                const isReplyToQualityMsg = qualityMek.message.extendedTextMessage?.contextInfo?.stanzaId === qualityMsgID;

                                                if (isReplyToQualityMsg && sender === qualityMek.key.remoteJid) {
                                                    const qualityNum = parseInt(qualityChoice) - 1;

                                                    if (isNaN(qualityNum) || qualityNum < 0 || qualityNum >= episodeDownloadLinks.length) {
                                                        await socket.sendMessage(sender, {
                                                            image:  { url: pickBotImage(sessionConfig) },
                                                            caption: formatMessage(
                                                                '❌ INVALID SELECTION',
                                                                `*වැරදි අංකයක්! 1-${episodeDownloadLinks.length} අතර තෝරන්න!*`,
                                                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                            )
                                                        }, { quoted: qualityMek });
                                                        return;
                                                    }

                                                    const selectedQuality = episodeDownloadLinks[qualityNum];

                                                    await socket.sendMessage(sender, { 
                                                        text: `⏳ 𝙂𝙚𝙩𝙩𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠...` 
                                                    }, { quoted: qualityMek });

                                                    try {
                                                        // 🟢 NEW: Download API - using selectedQuality.url (full ZT link)
                                                        let finalLink;
                                                        try {
                                                            const darkShanResponse = await cineGetWithRetry(`https://apis.laksidu.site/dl/cinesubz?url=${encodeURIComponent(selectedQuality.url)}&api_key=lakiyaofc2`);
                                                            const darkShanData = darkShanResponse.data;
                                                            if (!darkShanData.status || !darkShanData.data?.download) throw new Error('Failed to get download URL');
                                                            const finalNonTelegramLinks = darkShanData.data.download.filter(link => link.name && link.name.toLowerCase() !== 'telegram');
                                                            if (finalNonTelegramLinks.length === 0) throw new Error('No non-Telegram download links available');
                                                            finalLink = finalNonTelegramLinks.find(link => link.name === 'unknown') || finalNonTelegramLinks[0];
                                                        } catch (primaryErr) {
                                                            console.error('[cinesubz tv] primary download API failed, trying fallback:', primaryErr.message);
                                                            finalLink = await cineFallbackEpisodeLink(selectedItem.link, tvInfo.episodes?.list, selectedEpisode);
                                                        }

                                                        await socket.sendMessage(sender, { react: { text: '📥', key: qualityMek.key } });

                                                        await socket.sendMessage(sender, {
                                                            document: { url: finalLink.url },
                                                            mimetype: 'video/mp4',
                                                            fileName: `${tvInfo.title || 'Series'} S${selectedSeason.season}E${selectedEpisode.episode} - ${selectedEpisode.title}.mp4`,
                                                            caption: `*☘️ ${tvInfo.title || 'Series'} - ${selectedSeason.season}*

\`[Episode-${selectedEpisode.episode}]\`

${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                                                        }, { quoted: qualityMek });

                                                        await socket.sendMessage(sender, { react: { text: '✅', key: qualityMek.key } });

                                                    } catch (downloadError) {
                                                        console.error('Download error:', downloadError);
                                                        await socket.sendMessage(sender, {
                                                            image:  { url: pickBotImage(sessionConfig) },
                                                            caption: formatMessage(
                                                                '❌ DOWNLOAD ERROR',
                                                                `*Download link එක ලබාගැනීමේ දෝෂයක්.*\n${downloadError.message}`,
                                                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                            )
                                                        }, { quoted: qualityMek });
                                                    } finally {
                                                        socket.ev.off('messages.upsert', handleQualitySelect);
                                                        socket.ev.off('messages.upsert', handleEpisodeSelect);
                                                        socket.ev.off('messages.upsert', handleSeasonSelect);
                                                        socket.ev.off('messages.upsert', handleSelection);
                                                    }
                                                }
                                            };

                                            socket.ev.on('messages.upsert', handleQualitySelect);

                                        } catch (error) {
                                            console.error('Error fetching episode links:', error);
                                            await socket.sendMessage(sender, {
                                                image:  { url: pickBotImage(sessionConfig) },
                                                caption: formatMessage(
                                                    '❌ ERROR',
                                                    `*Download links ලබාගැනීමේ දෝෂයක්*\n${error.message}`,
                                                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                )
                                            }, { quoted: episodeMek });
                                            socket.ev.off('messages.upsert', handleEpisodeSelect);
                                            socket.ev.off('messages.upsert', handleSeasonSelect);
                                            socket.ev.off('messages.upsert', handleSelection);
                                        }
                                    }
                                };

                                socket.ev.on('messages.upsert', handleEpisodeSelect);
                            }
                        };

                        socket.ev.on('messages.upsert', handleSeasonSelect);

                    } catch (tvShowError) {
                        console.error('TV Show error:', tvShowError);
                        await socket.sendMessage(sender, {
                            image:  { url: pickBotImage(sessionConfig) },
                            caption: formatMessage(
                                '❌ ERROR',
                                `*TV series details ලබාගැනීමේ දෝෂයක්*\n${tvShowError.message}`,
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }

                } else {
                    await socket.sendMessage(sender, { 
                        text: '📽️ 𝙁𝙚𝙩????𝙞𝙣𝙜 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' 
                    }, { quoted: replyMek });

                    try {
                        const detailsResponse = await axios.get(`https://apis.laksidu.site/cinesubz/details?url=${encodeURIComponent(selectedItem.link)}&api_key=lakiyaofc2`);
                        const detailsData = detailsResponse.data;

                        if (!detailsData.status || !detailsData.data) {
                            throw new Error('Failed to fetch details');
                        }

                        const movieInfo = detailsData.data;

                        const validDownloads = movieInfo.downloads?.filter(dl => dl && dl.quality && dl.url) || [];

                        if (validDownloads.length === 0) {
                            await socket.sendMessage(sender, {
                                image:  { url: pickBotImage(sessionConfig) },
                                caption: formatMessage(
                                    '❌ NO DOWNLOADS',
                                    '*මෙම චිත්‍රපටය සඳහා බාගත කිරීමේ link නොමැත!*',
                                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                )
                            }, { quoted: replyMek });
                            return;
                        }

                        const description = movieInfo.description?.substring(0, 300) + (movieInfo.description?.length > 300 ? '...' : '') || 'No description available.';

                        const imdbRating = movieInfo.imdb_rating ? `${movieInfo.imdb_rating}/10` : 'N/A';
                        const year = movieInfo.year || 'N/A';
                        const runtime = movieInfo.runtime || 'N/A';
                        const director = movieInfo.director || 'N/A';
                        const country = movieInfo.country || 'N/A';
                        const cast = Array.isArray(movieInfo.cast) ? movieInfo.cast.join(', ') : movieInfo.cast || 'N/A';

                        const movieDetailsCaption = formatMessage(
                            `☘️ *𝗧ɪᴛʟᴇ ➟* _${movieInfo.title}_`,
                            `▫️🥇 *𝗜𝗺𝗱𝗯 𝗥ᴀᴛɪɴɢ ➟* _${imdbRating}_
▫️⏳ *𝗗ᴜʀᴀᴛɪᴏɴ ➟* _${runtime}_
▫️📅 *𝗥ᴇʟᴇᴀꜱᴇ 𝗬ᴇᴀʀ ➟* _${year}_
▫️🎬 *𝗗ɪʀᴇᴄᴛᴏʀ ➟* _${director}_
▫️🌎 *𝗖ᴏᴜɴᴛʀʏ ➟* _${country}_
▫️👥 *𝗖ᴀꜱᴛ ➟* _${cast}_
*➟➟➟➟➟➟➟➟➟➟*
*📖 𝗦𝗧𝗢𝗥𝗬 ➟*_${description}_`,
                            `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                        );

                        await socket.sendMessage(sender, {
                            image: { url: movieInfo.poster || sessionConfig.LAKIYA_IMAGE_PATH || config.LAKIYA_IMAGE_PATH },
                            caption: movieDetailsCaption
                        }, { quoted: replyMek });

                        const downloadOptionsCaption = formatMessage(
                            `⬇️🍀 *𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦*`,
                            `${validDownloads.map((dl, i) => `▫️ *${(i + 1).toString().padStart(2, '0')} ❱❱ 📥 ${dl.quality}*`).join('\n')}\n

╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        );

                        const downloadOptionsMsg = await socket.sendMessage(sender, {
                            text: downloadOptionsCaption
                        }, { quoted: replyMek });

                        const optionsMsgID = downloadOptionsMsg.key.id;

                        const handleDownload = async ({ messages: downloadMessages }) => {
                            const downloadMek = downloadMessages[0];
                            if (!downloadMek?.message) return;

                            const downloadChoice = downloadMek.message.conversation || downloadMek.message.extendedTextMessage?.text;
                            const isReplyToOptionsMsg = downloadMek.message.extendedTextMessage?.contextInfo?.stanzaId === optionsMsgID;

                            if (isReplyToOptionsMsg && sender === downloadMek.key.remoteJid) {
                                const choiceNum = parseInt(downloadChoice) - 1;

                                if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= validDownloads.length) {
                                    await socket.sendMessage(sender, {
                                        image:  { url: pickBotImage(sessionConfig) },
                                        caption: formatMessage(
                                            '❌ INVALID SELECTION',
                                            `*වැරදි අංකයක්! 1-${validDownloads.length} අතර තෝරන්න!*`,
                                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                        )
                                    }, { quoted: downloadMek });
                                    return;
                                }

                                const selectedDownload = validDownloads[choiceNum];

                                await socket.sendMessage(sender, { 
                                    text: `⏳ 𝙂𝙚𝙩𝙩𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠...` 
                                }, { quoted: downloadMek });

                                try {
                                    let downloadData, preferredLink;
                                    try {
                                        const downloadResponse = await cineGetWithRetry(`https://apis.laksidu.site/dl/cinesubz?url=${encodeURIComponent(selectedDownload.url)}&api_key=lakiyaofc2`);
                                        downloadData = downloadResponse.data;
                                        if (!downloadData.status || !downloadData.data?.download) throw new Error('Failed to get download URL');
                                        const nonTelegramLinks = downloadData.data.download.filter(link => link.name && link.name.toLowerCase() !== 'telegram');
                                        if (nonTelegramLinks.length === 0) throw new Error('No non-Telegram download links available');
                                        preferredLink = nonTelegramLinks.find(link => link.name === 'unknown') || nonTelegramLinks[0];
                                    } catch (primaryErr) {
                                        console.error('[cinesubz] primary download API failed, trying fallback:', primaryErr.message);
                                        const fb = await cineFallbackMovieLink(selectedItem.link, selectedDownload.quality, choiceNum);
                                        downloadData = { data: {} };
                                        preferredLink = { url: fb.url };
                                    }

                                    await socket.sendMessage(sender, { react: { text: '📥', key: downloadMek.key } });

                                    await socket.sendMessage(sender, {
                                        document: { url: preferredLink.url },
                                        mimetype: 'video/mp4',
                                        fileName: downloadData.data.title || `${movieInfo.title} ${selectedDownload.quality}.mp4`,
                                        caption: formatMessage(
                                            `☘️ ${movieInfo.title}`,
                                            `\`❚█═${sessionConfig.MOVIE_CAPTION || config.MOVIE_CAPTION}═█❚\`
                                            
\`[WEB-DL-${selectedDownload.quality}]\``,
                                            `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                                        )
                                    }, { quoted: downloadMek });

                                    await socket.sendMessage(sender, { react: { text: '✅', key: downloadMek.key } });

                                } catch (downloadError) {
                                    console.error('Download link error:', downloadError);
                                    await socket.sendMessage(sender, {
                                        image:  { url: pickBotImage(sessionConfig) },
                                        caption: formatMessage(
                                            '❌ DOWNLOAD ERROR',
                                            `*Download link එක ලබාගැනීමේ දෝෂයක්.*\n${downloadError.message}`,
                                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                        )
                                    }, { quoted: downloadMek });
                                } finally {
                                    socket.ev.off('messages.upsert', handleDownload);
                                    socket.ev.off('messages.upsert', handleSelection);
                                }
                            }
                        };

                        socket.ev.on('messages.upsert', handleDownload);

                    } catch (detailsError) {
                        console.error('Details error:', detailsError);
                        await socket.sendMessage(sender, {
                            image:  { url: pickBotImage(sessionConfig) },
                            caption: formatMessage(
                                '❌ ERROR',
                                `*Details ලබාගැනීමේ දෝෂයක්*\n${detailsError.message}`,
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }
                }
            }
        };

        socket.ev.on('messages.upsert', handleSelection);

    } catch (error) {
        console.error('Cinezub command error:', error);
        await socket.sendMessage(sender, {
            image:  { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                `*දෝෂයක් ඇතිවුණා:* ${error.message || 'Unknown error'}`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }

    break;
                case 'sinhalasub':
    if (!args.length) {
        await socket.sendMessage(sender, {
             image: { url: pickBotImage(sessionConfig)},
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .sinhalasub spider*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const movieQuery55 = args.join(' ');

    await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));


    let sinhalasubSelectionListener = null;
    let sinhalasubDownloadListener = null;
    let sinhalasubSelectionTimeout = null;
    let sinhalasubDownloadTimeout = null;


    let sinhalasubMasterTimeout = null;
    const clearAllSinhalasubListeners = () => {
        console.log('🧹 Clearing all Sinhalasub listeners');


        if (sinhalasubSelectionListener) {
            socket.ev.off('messages.upsert', sinhalasubSelectionListener);
            sinhalasubSelectionListener = null;
        }
        if (sinhalasubSelectionTimeout) {
            clearTimeout(sinhalasubSelectionTimeout);
            sinhalasubSelectionTimeout = null;
        }


        if (sinhalasubDownloadListener) {
            socket.ev.off('messages.upsert', sinhalasubDownloadListener);
            sinhalasubDownloadListener = null;
        }
        if (sinhalasubDownloadTimeout) {
            clearTimeout(sinhalasubDownloadTimeout);
            sinhalasubDownloadTimeout = null;
        }


        if (sinhalasubMasterTimeout) {
            clearTimeout(sinhalasubMasterTimeout);
            sinhalasubMasterTimeout = null;
        }
    };

    try {
        const searchResponse = await axios.get(`${config.API_MAIN_URL}/sinhalasub/search?query=${encodeURIComponent(movieQuery55)}&api_key=${config.API_KEY}`);
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.data?.results || searchData.data.results.length === 0) {
            await socket.sendMessage(sender, {
                 image: { url: pickBotImage(sessionConfig)},
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*චිත්‍රපට හමුවෙන්නේ නැත! 😞*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const movies = searchData.data.results.slice(0, 115);
        let listText = `🎀 *𝗦𝗘𝗔𝗥𝗖𝗛 : _${movieQuery55}_*
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤
╭──────●➤\n`;

        movies.forEach((movie, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${movie.title}*\n`;
        });

        listText += `╰──────────●➤\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig)},
            caption: listText
        }, { quoted: msg });

        const messageID = sentMsg.key.id;


        sinhalasubMasterTimeout = setTimeout(() => {
            clearAllSinhalasubListeners();
            console.log('🧹 Sinhalasub master timeout - All listeners cleared after 3 minutes');
        }, 180000);


        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {

                if (sinhalasubSelectionTimeout) {
                    clearTimeout(sinhalasubSelectionTimeout);
                    sinhalasubSelectionTimeout = null;
                }


                sinhalasubSelectionTimeout = setTimeout(() => {
                    if (sinhalasubSelectionListener) {
                        socket.ev.off('messages.upsert', sinhalasubSelectionListener);
                        sinhalasubSelectionListener = null;
                        console.log('🧹 Sinhalasub selection listener timeout');
                    }
                    sinhalasubSelectionTimeout = null;
                }, 120000);

                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= movies.length) {
                    await socket.sendMessage(sender, {
                         image: { url: pickBotImage(sessionConfig)},
                        caption: formatMessage(
                            '❌ INVALID SELECTION',
                            `*වැරදි අංකයක්! 1-${movies.length} අතර තෝරන්න! 😕*`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                    return;
                }

                const selectedMovie = movies[choice];

                await socket.sendMessage(sender, { 
                    text: '📽️ 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' 
                }, { quoted: replyMek });


                await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

                try {
                    const infoResponse = await axios.get(`${config.API_MAIN_URL}/sinhalasub/info?url=${encodeURIComponent(selectedMovie.url)}&api_key=${config.API_KEY}`);
                    const infoData = infoResponse.data;

                    if (!infoData.status || !infoData.data) {
                        throw new Error('Failed to fetch movie details');
                    }

                    const movieInfo = infoData.data.movie;
                    const downloads = infoData.data.downloads || [];


                    const videoDownloads = downloads.filter(d => d.server === 'pixeldrain');

                    if (videoDownloads.length === 0) {
                        await socket.sendMessage(sender, {
                             image: { url: pickBotImage(sessionConfig)},
                            caption: formatMessage(
                                '❌ NO DOWNLOADS',
                                '*Pixeldrain බාගත කිරීම් නොමැත!*',
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: replyMek });
                        return;
                    }

                    const castPreview = movieInfo.cast?.slice(0, 5).join(', ') + (movieInfo.cast?.length > 5 ? '...' : '');

                    const detailsCaption = formatMessage(
                        `🍀 *𝗧ɪᴛʟᴇ : ${movieInfo.title}`,
                        `▫️📅 *𝗥ᴇʟᴇᴀꜱᴇ 𝗬ᴇᴀʀ ➟ ${movieInfo.year || 'N/A'}*
▫️🥇 *𝗜𝗺𝗱ʙ 𝗥ᴀᴛɪɴɢ ➟ ${movieInfo.rating || 'N/A'}/10*
▫️📊 *𝗤ᴜᴀʟɪᴛʏ ➟ ${movieInfo.quality || 'N/A'}*
▫️⏳ *𝗗ᴜʀᴀᴛɪᴏɴ ➟ ${movieInfo.runtime || 'N/A'}*
▫️🔠 *𝗟ᴀɴɢᴜᴀɢᴇ ➟ ${movieInfo.language || 'N/A'}*
▫️🎭 *𝗚ᴇɴʀᴇꜱ ➟ ${movieInfo.genres?.join(', ') || 'N/A'}*
▫️🙅 *𝗗ɪʀᴇᴄᴛᴏʀ ➟ ${movieInfo.director?.slice(0,2).join(', ') || 'N/A'}*
▫️👥 *𝗖ᴀꜱᴛ ➟ ${castPreview || 'N/A'}*
▫️👨‍💻 *𝗦ᴜʙᴛɪᴛʟᴇ ➟ ${movieInfo.subtitle?.author || 'Sinhala'} (${movieInfo.subtitle?.site || 'Baiscope'})*
▫️📖 *sᴛᴏʀʏ ➟ ${movieInfo.description?.substring(0, 150) || 'No description'}...*
▫️🔗 *Jᴏɪɴ ➟ ${sessionConfig.MGROUP_LINK || config.MGROUP_LINK}*`,
                        `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                    );

                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: movieInfo.poster || selectedMovie.poster || pickBotImage(sessionConfig) },
                        caption: detailsCaption
                    }, { quoted: replyMek });


                    await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

                    const downloadOptionsText = `*⬇️🎀 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦*
*Reply with number 👇*

${videoDownloads.map((d, i) => 
`*🔰 ${i + 1} ┃ 📥 ${d.quality || 'N/A'} • ${d.size || 'N/A'}*`
).join('\n')}

${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

                    const downloadMsg = await socket.sendMessage(sender, {
                        text: downloadOptionsText
                    }, { quoted: infoMsg });

                    const infoMsgID = downloadMsg.key.id;


                    const handleDownload = async ({ messages: downloadMessages }) => {
                        const downloadMek = downloadMessages[0];
                        if (!downloadMek?.message) return;

                        const downloadChoice = downloadMek.message.conversation || downloadMek.message.extendedTextMessage?.text;
                        const isReplyToInfoMsg = downloadMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;

                        if (isReplyToInfoMsg && sender === downloadMek.key.remoteJid) {

                            if (sinhalasubDownloadTimeout) {
                                clearTimeout(sinhalasubDownloadTimeout);
                                sinhalasubDownloadTimeout = null;
                            }


                            sinhalasubDownloadTimeout = setTimeout(() => {
                                if (sinhalasubDownloadListener) {
                                    socket.ev.off('messages.upsert', sinhalasubDownloadListener);
                                    sinhalasubDownloadListener = null;
                                    console.log('🧹 Sinhalasub download listener timeout');
                                }
                                sinhalasubDownloadTimeout = null;
                            }, 120000);

                            const choiceNum = parseInt(downloadChoice) - 1;

                            if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= videoDownloads.length) {
                                await socket.sendMessage(sender, {
                                     image: { url: pickBotImage(sessionConfig)},
                                    caption: formatMessage(
                                        '❌ INVALID SELECTION',
                                        `*වැරදි අංකයක්! 1-${videoDownloads.length} අතර තෝරන්න!*`,
                                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                    )
                                }, { quoted: downloadMek });
                                return;
                            }

                            const selectedDownload = videoDownloads[choiceNum];

                            await socket.sendMessage(sender, { 
                                text: `⏳ 𝙂𝙚𝙩𝙩𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠...` 
                            }, { quoted: downloadMek });


                            await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

                            try {

                                const downloadResponse = await axios.get(`${config.API_MAIN_URL}/sinhalasub/download2?url=${encodeURIComponent(selectedDownload.link_page)}&api_key=${config.API_KEY}`);
                                const downloadData = downloadResponse.data;

                                if (!downloadData.status || !downloadData.data?.download) {
                                    throw new Error('Failed to get download URL');
                                }

                                const finalDownloadUrl = downloadData.data.download;
                                const fileInfo = downloadData.data.file_info || {};


                                let fileName = fileInfo.name || `${movieInfo.title} [${selectedDownload.quality || 'Unknown'}].mp4`;
                                const mimeType = fileInfo.mimeType || 'video/mp4';

                                console.log('Download URL:', finalDownloadUrl);
                                console.log('File Name:', fileName);
                                console.log('Mime Type:', mimeType);

                                await socket.sendMessage(sender, { react: { text: '📥', key: downloadMek.key } });




                                let sizeText = 'N/A';
                                if (fileInfo.size) {
                                    const sizeInMB = fileInfo.size / 1024 / 1024;
                                    if (sizeInMB > 1024) {
                                        sizeText = (sizeInMB / 1024).toFixed(2) + ' GB';
                                    } else {
                                        sizeText = sizeInMB.toFixed(2) + ' MB';
                                    }
                                }


                                await socket.sendMessage(sender, {
                                    document: { url: finalDownloadUrl },
                                    mimetype: mimeType,
                                    fileName: fileName,
                                    caption: formatMessage(
                                        `🍀 ${movieInfo.title}`,
                                        `\`❚█${sessionConfig.MOVIE_CAPTION || config.MOVIE_CAPTION}█❚\`

\`❪${selectedDownload.quality || 'Unknown'}❫\``,
                                        `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                                    )
                                }, { quoted: downloadMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: downloadMek.key } });


                                clearAllSinhalasubListeners();

                            } catch (downloadError) {
                                console.error('Download link error:', downloadError);
                                await socket.sendMessage(sender, {
                                     image: { url: pickBotImage(sessionConfig)},
                                    caption: formatMessage(
                                        '❌ DOWNLOAD ERROR',
                                        `*Download link එක ලබාගැනීමේ දෝෂයක්.*\nError: ${downloadError.message}`,
                                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                    )
                                }, { quoted: downloadMek });
                            }
                        }
                    };


                    sinhalasubDownloadListener = handleDownload;
                    socket.ev.on('messages.upsert', handleDownload);


                    sinhalasubDownloadTimeout = setTimeout(() => {
                        if (sinhalasubDownloadListener) {
                            socket.ev.off('messages.upsert', sinhalasubDownloadListener);
                            sinhalasubDownloadListener = null;
                            console.log('🧹 Sinhalasub download listener timeout - cleaned up');
                        }
                        sinhalasubDownloadTimeout = null;
                    }, 120000);

                } catch (infoError) {
                    console.error('Movie info error:', infoError);
                    await socket.sendMessage(sender, {
                         image: { url: pickBotImage(sessionConfig)},
                        caption: formatMessage(
                            '❌ ERROR',
                            `*Movie details ලබාගැනීමේ දෝෂයක්:* ${infoError.message}`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                }
            }
        };


        sinhalasubSelectionListener = handleSelection;
        socket.ev.on('messages.upsert', handleSelection);


        sinhalasubSelectionTimeout = setTimeout(() => {
            if (sinhalasubSelectionListener) {
                socket.ev.off('messages.upsert', sinhalasubSelectionListener);
                sinhalasubSelectionListener = null;
                console.log('🧹 Sinhalasub selection listener timeout - cleaned up');
            }
            sinhalasubSelectionTimeout = null;
        }, 120000);

    } catch (error) {
        console.error('Movie command error:', error);

        clearAllSinhalasubListeners();
        await socket.sendMessage(sender, {
             image: { url: pickBotImage(sessionConfig)},
            caption: formatMessage(
                '❌ ERROR',
                `*දෝෂයක් ඇතිවුණා:* ${error.message || 'Unknown error'}`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }
    break;

    break;    case 'menu':
case 'help': {
    try {
        const P = sessionConfig.PREFIX || config.PREFIX || '.';
        const pushName = msg.pushName || 'User';
        const nowSL = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Colombo' }));
        const pad = (n) => String(n).padStart(2, '0');
        const dateStr = `${nowSL.getFullYear()}/${pad(nowSL.getMonth() + 1)}/${pad(nowSL.getDate())}`;
        const timeStr = nowSL.toLocaleTimeString('en-US', { hour12: true });
        const hr = nowSL.getHours();
        const greet = hr < 5 ? 'Good Night 🌙' : hr < 12 ? 'Good Morning ✨' : hr < 17 ? 'Good Afternoon 🚀' : hr < 20 ? 'Good Evening 🌇' : 'Good Night 🌙';
        const upSec = process.uptime();
        const upStr = `${Math.floor(upSec / 3600)}h ${Math.floor((upSec % 3600) / 60)}m`;
        const ramMB = (process.memoryUsage().rss / 1024 / 1024).toFixed(0);
        const botName = sessionConfig.BOT_NAME || config.BOT_NAME || 'SHAGGY XMD';
        const footer = sessionConfig.BOT_FOOTER || config.BOT_FOOTER;
        const menuImage = { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE }; // menu image eka wenas karanne na

        // ── Categories (every command listed here has a real case) ──
        const categories = {
            '1': {
                emoji: '🎬', title: 'MOVIES & TV',
                cmds: [
                    ['cinesubz', 'Sinhala sub movies'],
                    ['sinhalasub', 'Sinhala sub movies'],
                    ['cineverse', 'CineVerseLK movies & series'],
                    ['cinetv', 'TV series'],
                    ['cinesend', 'CineSubz -> send to group (owner)'],
                    ['tvsend', 'CineTV -> send to group (owner)'],
                    ['cin', 'Cine movies (alt)'],
                    ['movie', 'Multi source search'],
                    ['movieall', 'All-source search'],
                    ['moviesublk', 'MovieSubLK'],
                    ['subzlk', 'SubzLK'],
                    ['moviemania', 'MovieMania'],
                    ['lakvision', 'LakVision'],
                    ['piratelk', 'PirateLK'],
                    ['dinka', 'DinkaMovies'],
                    ['cinemx', 'CineMX'],
                    ['pupilmovie', 'PupilVideo'],
                    ['chithrapata', 'Chithrapata'],
                    ['dubzone', 'DubZone'],
                    ['sinhalatop', 'SinhalaTop'],
                    ['thinkiri', 'TheNkiri'],
                    ['tamilmv', 'Tamil movies'],
                    ['nethmv', 'NethMV vault'],
                    ['mhbd', 'MovieHubBD'],
                    ['wrestling', 'WatchWrestling']
                ]
            },
            '2': {
                emoji: '⬇️', title: 'DOWNLOADERS',
                cmds: [
                    ['song', 'Song / YouTube audio'],
                    ['csong', 'Song -> channel post (owner)'],
                    ['yt', 'YouTube video / audio'],
                    ['tiktok', 'TikTok (no watermark)'],
                    ['mediafire', 'MediaFire files  🆕'],
                    ['gdrive', 'Google Drive files'],
                    ['apk', 'Mod APK search & dl'],
                    ['rom', 'Game ROM search & dl'],
                    ['fitgirl', 'FitGirl repacks'],
                    ['fginfo', 'FitGirl game info'],
                    ['sdl', 'Save status video'],
                    ['vv', 'View-once saver']
                ]
            },
            '3': {
                emoji: '🎌', title: 'ANIME & CARTOON',
                cmds: [
                    ['anime', 'Anime search'],
                    ['animexin', 'Donghua / Animexin'],
                    ['cartoon', 'Cartoons.lk'],
                    ['cartoon2', 'Sinhala cartoons']
                ]
            },
            '4': {
                emoji: '🛠️', title: 'TOOLS & FUN',
                cmds: [
                    ['ai', 'AI chat'],
                    ['sticker', 'Image → sticker'],
                    ['toimg', 'Sticker → image'],
                    ['qr', 'QR code generator'],
                    ['short', 'URL shortener'],
                    ['weather', 'Weather report'],
                    ['calc', 'Calculator'],
                    ['news', 'Sri Lanka news'],
                    ['papers', 'Past papers'],
                    ['schedule', 'Schedule a message'],
                    ['jid', 'Get chat JID']
                ]
            },
            '5': {
                emoji: '⚙️', title: 'GENERAL & GROUP',
                cmds: [
                    ['alive', 'Bot status'],
                    ['menu', 'This menu'],
                    ['ping', 'Speed & system info'],
                    ['owner', 'Owner contact'],
                    ['pair', 'Pair a new number'],
                    ['getkey', 'Web panel access key'],
                    ['license', 'Subscription status'],
                    ['bots', 'Active sessions'],
                    ['tagall', 'Tag everyone (group)'],
                    ['groupinfo', 'Group info']
                ]
            },
            '6': {
                emoji: '👑', title: 'ADMIN',
                adminOnly: true,
                note: '\n⚠️ _Admin / owner only_',
                cmds: [
                    ['set', 'Settings panel'],
                    ['adauto', 'Add auto reply'],
                    ['delauto', 'Delete auto reply'],
                    ['autorep', 'Auto reply list'],
                    ['add', 'Add subscription (master)']
                ]
            }
        };

        const total = Object.values(categories).reduce((n, c) => n + c.cmds.length, 0);
        const catLines = Object.entries(categories)
            .map(([k, c]) => `│ ${k}\uFE0F\u20E3 ${c.emoji} ${c.title}  ·  ${c.cmds.length}`)
            .join('\n');

        const btnOn = () => sessionConfig.BUTTON_MODE === 'true' && buttonAllowed();
        const toggleKey = String(Object.keys(categories).length + 1);
        const buildMain = () =>
`╭━━━━━━━━━━━━━━━━━━━╮
┃  ✦ *${botName}* ✦
╰━━━━━━━━━━━━━━━━━━━╯
👋 Hey *${pushName}*!   ${greet}

╭─「 📊 *STATUS* 」
│ ⏰ Time    : ${timeStr}
│ 📆 Date    : ${dateStr}
│ ⚡ Uptime  : ${upStr}
│ 💾 RAM     : ${ramMB} MB
│ 🔣 Prefix  : ${P}
│ 🌐 Mode    : ${sessionConfig.MODE || config.MODE}
│ 🧩 Cmds    : ${total}
╰──────────────────

╭─「 📂 *MENU LIST* 」
${catLines}
│ ${toggleKey}\uFE0F\u20E3 🔘 BUTTON MODE  ·  ${!buttonAllowed() ? '🔒 LOCKED' : btnOn() ? 'ON ✅' : 'OFF ❌'}
╰──────────────────
💬 _Reply to this message with a number (1-${toggleKey})_

> ${footer}`;

        const renderCategory = (c) => {
            const rows = c.cmds.map(([cmd, desc]) => `│ ➤ *${P}${cmd}*\n│    ↳ _${desc}_`).join('\n');
            return `╭─「 ${c.emoji} *${c.title}* 」\n${rows}\n╰──────────────────${c.note || ''}\n\n↩️ Reply *0* — back to main menu\n\n> ${footer}`;
        };

        const menuIds = new Set();
        const sentMsg = await socket.sendMessage(sender, { image: menuImage, caption: buildMain() }, { quoted: msg });
        if (sentMsg?.key?.id) menuIds.add(sentMsg.key.id);

        const originalSender = (msg.key.participant || msg.key.remoteJid || '').split('@')[0].split(':')[0];

        // ── Reply listener (works on the main menu AND on category messages) ──
        const handleMenuReply = async ({ messages: replyMsgs }) => {
            try {
                const replyMek = replyMsgs?.[0];
                if (!replyMek?.message) return;

                const ctx = replyMek.message.extendedTextMessage?.contextInfo;
                if (!ctx || !menuIds.has(ctx.stanzaId)) return;

                const replier = (replyMek.key.participant || replyMek.key.remoteJid || '').split('@')[0].split(':')[0];
                if (replier !== originalSender) return;

                const replyText = (replyMek.message.extendedTextMessage?.text || '').trim();

                if (replyText === '0') {
                    const s = await socket.sendMessage(sender, { image: menuImage, caption: buildMain() }, { quoted: replyMek });
                    if (s?.key?.id) menuIds.add(s.key.id);
                    return;
                }

                // 🔘 Button mode toggle (owner / admin only)
                if (replyText === toggleKey) {
                    const ADMIN_NUMBERS_T = (process.env.ADMIN_NUMBERS || '').split(',').map(n => n.trim()).filter(Boolean);
                    if (!isOwner && !ADMIN_NUMBERS_T.includes(senderNumber)) {
                        await socket.sendMessage(sender, { text: '❌ *Admin only!*' }, { quoted: replyMek });
                        return;
                    }
                    if (!buttonAllowed()) {
                        await socket.sendMessage(sender, { text: buttonLockedMsg }, { quoted: replyMek });
                        return;
                    }
                    const newVal = btnOn() ? 'false' : 'true';
                    sessionConfig = { ...sessionConfig, BUTTON_MODE: newVal };
                    await updateUserConfig(sanitizedNumber, sessionConfig);
                    activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });
                    await socket.sendMessage(sender, { text: `🔘 *Button Mode ${newVal === 'true' ? 'ON ✅' : 'OFF ❌'}*` }, { quoted: replyMek });
                    const sm = await socket.sendMessage(sender, { image: menuImage, caption: buildMain() }, { quoted: replyMek });
                    if (sm?.key?.id) menuIds.add(sm.key.id);
                    return;
                }

                const cat = categories[replyText];
                if (!cat) return;

                if (cat.adminOnly) {
                    const ADMIN_NUMBERS = (process.env.ADMIN_NUMBERS || '').split(',').map(n => n.trim()).filter(Boolean);
                    if (!isOwner && !ADMIN_NUMBERS.includes(senderNumber)) {
                        await socket.sendMessage(sender, { text: '❌ *Admin only!*' }, { quoted: replyMek });
                        return;
                    }
                }

                let s;
                if (btnOn()) {
                    try {
                        const rows = cat.cmds.map(([cmd, desc]) => ({ title: `${P}${cmd}`, description: desc, id: `BTN|CMD|${cmd}` }));
                        rows.push({ title: '⬅️ Back to menu', description: 'Main menu ekata', id: 'BTN|NUM|{ID}|0' });
                        s = await sendList(socket, sender, {
                            text: `╭─「 ${cat.emoji} *${cat.title}* 」\n│ 👇 Command ekak select karanna\n╰──────────────────${cat.note || ''}\n\n> ${footer}`,
                            buttonText: '📋 Commands',
                            rows,
                            quoted: replyMek
                        });
                    } catch (btnErr) {
                        console.error('Menu button list failed, using text:', btnErr.message);
                        s = null;
                    }
                }
                if (!s) s = await socket.sendMessage(sender, { text: renderCategory(cat) }, { quoted: replyMek });
                if (s?.key?.id) menuIds.add(s.key.id);
            } catch (err) {
                console.error('Menu reply error:', err.message);
            }
        };

        socket.ev.on('messages.upsert', handleMenuReply);

        // ── Auto cleanup after 2 min ──
        setTimeout(() => {
            socket.ev.off('messages.upsert', handleMenuReply);
        }, 120000);

    } catch (e) {
        console.error('Menu error:', e.message);
    }
    break;
}

case 'alive': {
    const upSec = process.uptime();
    const upStr = `${Math.floor(upSec / 3600)}h ${Math.floor((upSec % 3600) / 60)}m ${Math.floor(upSec % 60)}s`;
    const P = sessionConfig.PREFIX || config.PREFIX || '.';
    const footer = sessionConfig.BOT_FOOTER || config.BOT_FOOTER;
    await socket.sendMessage(sender, {
        image: { url: pickBotImage(sessionConfig) },
        caption: formatMessage(
            '✅ I AM ALIVE',
            `⚡ *Uptime :* ${upStr}\n🔣 *Prefix :* ${P}\n🌐 *Mode :* ${sessionConfig.MODE || config.MODE}\n\n💬 Type *${P}menu* to see all commands`,
            footer
        )
    }, { quoted: msg });
    break;
}

case 'cinesend': {
    if (!isOwner) {
        await socket.sendMessage(sender, { text: '❌ *Owner witharai meka use karanna puluwan.*' }, { quoted: msg });
        break;
    }
    const gsArgs = args.join(' ');
    const gsComma = gsArgs.indexOf(',');
    const gsLinkInput = gsComma === -1 ? '' : gsArgs.slice(0, gsComma).trim();
    const gsQuery = gsComma === -1 ? '' : gsArgs.slice(gsComma + 1).trim();
    if (!gsLinkInput || !gsQuery) {
        await socket.sendMessage(sender, {
            text: `❌ *Use karana widiya:*\n\n\`${sessionConfig.PREFIX || config.PREFIX || '.'}cinesend <group link>, <movie / series name>\`\n\n*Example:*\n${sessionConfig.PREFIX || config.PREFIX || '.'}cinesend https://chat.whatsapp.com/xxxxx, spider man\n\n_Bot number eka e group ekata add wela inna one._`
        }, { quoted: msg });
        break;
    }
    let targetGroupJid = null;
    let targetGroupName = 'Group';
    try {
        const gsCode = gsLinkInput.includes('chat.whatsapp.com/') ? gsLinkInput.split('chat.whatsapp.com/')[1].split('?')[0].trim() : gsLinkInput;
        const gsInfo = await socket.groupGetInviteInfo(gsCode);
        targetGroupJid = gsInfo.id;
        targetGroupName = gsInfo.subject || targetGroupName;
        await socket.groupMetadata(targetGroupJid); // bot eka e group eke inna one
    } catch (gsErr) {
        await socket.sendMessage(sender, {
            text: `❌ *Group ekata send karanna baha.*\n\n_Link eka waradi, ho bot eka e group ekata add wela na._\n${gsErr.message}`
        }, { quoted: msg });
        break;
    }
    await socket.sendMessage(sender, { text: `📤 *Send to :* ${targetGroupName}` }, { quoted: msg });

    const cinezubQuerytv = gsQuery;
    await socket.sendMessage(sender, { text: '📽️ 𝙎𝙚𝙖𝙧𝙘𝙝𝙞𝙣𝙜 𝙤𝙣 𝘾𝙞𝙣𝙚𝙨𝙪𝙗𝙯...' });

    try {
        const searchResponse = await axios.get(`https://apis.laksidu.site/cinesubz/search?query=${encodeURIComponent(cinezubQuerytv)}&api_key=lakiyaofc2`);
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.results || searchData.results.length === 0) {
            await socket.sendMessage(sender, {
                image:  { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*cinesubz හි චිත්‍රපට හමුවෙන්නේ නැත! 😞*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const cinezubResults = searchData.results.slice(0, 25);
        let listText = `☘️ *𝗠𝗢𝗩𝗜𝗘 : _𝗦𝗘𝗔𝗥𝗖𝗛 𝗥𝗘𝗦𝗨𝗟𝗧𝗦_* 🔍
╭──────●➤
🔎 *𝗤𝘂𝗲𝗿𝘆 ➟* _${cinezubQuerytv}_
📊 *Status ➟* _Results Found_
╰──────────●➤
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤
💡 *𝗥ᴇᴘʟʏ ᴡɪᴛʜ ᴀ 𝗡ᴜᴍʙᴇʀ 𝘁ᴏ 𝗦ᴇʟᴇᴄ𝘛*
*╭──────●➤*\n\n`;

        cinezubResults.forEach((item, index) => {
            const type = item.link.includes('/tvshows/') ? '📺 TV Series' : '🎬 Movie';
            listText += `*🍟${index + 1} ║❯❯ ${type} | ${item.title}*\n`;
        });

        listText += `╰──────────●➤\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: config.BOT_IMAGE},
            caption: listText
        }, { quoted: msg });

        const messageID = sentMsg.key.id;

        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= cinezubResults.length) {
                    await socket.sendMessage(sender, {
                        image:  { url: pickBotImage(sessionConfig) },
                        caption: formatMessage(
                            '❌ INVALID SELECTION',
                            `*වැරදි අංකයක්! 1-${cinezubResults.length} අතර තෝරන්න! 😕*`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                    return;
                }

                const selectedItem = cinezubResults[choice];
                const isTvShow = selectedItem.link.includes('/tvshows/');

                if (isTvShow) {
                    await socket.sendMessage(sender, { 
                        text: '📺 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙏𝙑 𝙨𝙚𝙧𝙞𝙚𝙨 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' 
                    }, { quoted: replyMek });

                    try {
                        const tvShowResponse = await axios.get(`https://apis.laksidu.site/cinesubz/tvshow?url=${encodeURIComponent(selectedItem.link)}&api_key=lakiyaofc2`);
                        const tvShowData = tvShowResponse.data;

                        if (!tvShowData.status || !tvShowData.data) {
                            throw new Error('Failed to fetch TV show details');
                        }

                        const tvInfo = tvShowData.data;

                        // 🟢 NEW STRUCTURE - Extract data correctly
                        const rating = tvInfo.rating?.score || 'N/A';
                        const totalEpisodes = tvInfo.episodes?.total || 'N/A';
                        const episodesList = tvInfo.episodes?.list || [];

                        // Group episodes by season (extract season from episode title or URL)
                        const seasonsMap = {};
                        episodesList.forEach(ep => {
                            let seasonNum = '1';
                            // Try to extract season from episode number or title
                            const seasonMatch = ep.number?.match(/^(\d+)/);
                            if (seasonMatch) {
                                seasonNum = seasonMatch[1];
                            }
                            if (!seasonsMap[seasonNum]) {
                                seasonsMap[seasonNum] = [];
                            }
                            seasonsMap[seasonNum].push(ep);
                        });

                        const seasonsArray = Object.keys(seasonsMap).map(season => ({
                            season: parseInt(season),
                            total_episodes: seasonsMap[season].length,
                            episodes: seasonsMap[season].map(ep => ({
                                episode: ep.number || '1',
                                title: ep.title || 'Episode',
                                url: ep.url || ''
                            }))
                        }));

                        const totalSeasons = seasonsArray.length;

                        let tvDetailsText = 
    `☘️ *𝗧ɪᴛʟᴇ ➟* _${tvInfo.title || 'N/A'}_
▫️🥇 *𝗜𝗺𝗱𝗯 𝗥ᴀᴛɪɴɢ ➟*  _${rating}_
▫️📅 *𝗥ᴇʟᴇᴀꜱᴇ 𝗬ᴇᴀʀ ➟*_${tvInfo.year || 'N/A'}_
▫️📀 *𝗦ᴇᴀꜱᴏɴꜱ ➟* _${totalSeasons} Total_
▫️📊 *𝗘ᴘɪꜱᴏᴅᴇꜱ ➟* _${totalEpisodes} Total_
*➟➟➟➟➟➟➟➟➟➟*
📖 *𝗦𝗧𝗢𝗥𝗬*_${tvInfo.description?.substring(0, 30) || 'No description available.'}..._`;

                        await socket.sendMessage(sender, {
                            image: { url: tvInfo.poster || sessionConfig.LAKIYA_IMAGE_PATH || config.LAKIYA_IMAGE_PATH },
                            caption: tvDetailsText
                        }, { quoted: replyMek });

                        let seasonsText = 
    `☘️ *𝗧𝗩-𝗦𝗘𝗥𝗜𝗘𝗦 : _𝗦𝗘𝗔𝗦𝗢𝗡 𝗦𝗘𝗟𝗘𝗖𝗧𝗜𝗢𝗡_* 📺
*➟➟➟➟➟➟➟➟➟➟*
⬇️🍀 *𝗦𝗘𝗟𝗘𝗖𝗧 𝗬𝗢𝗨𝗥 𝗦𝗘𝗔𝗦𝗢𝗡*
*➟➟➟➟➟➟➟➟➟➟*
💡 *𝗥ᴇᴘʟʏ ᴡɪᴛʜ ᴀ 𝗡ᴜᴍʙᴇʀ 𝘁ᴏ 𝗦ᴇʟᴇᴄ𝘛*
*➟➟➟➟➟➟➟➟➟➟*\n\n`;

                        seasonsArray.forEach((season, idx) => {
                            seasonsText += `🍀 *${idx + 1} ┃》📀 Season ${season.season} (${season.total_episodes} episodes)*\n`;
                        });

                        seasonsText += `\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

                        const seasonMsg = await socket.sendMessage(sender, {
                            text: seasonsText
                        }, { quoted: replyMek });

                        const seasonMsgID = seasonMsg.key.id;

                        const handleSeasonSelect = async ({ messages: seasonMessages }) => {
                            const seasonMek = seasonMessages[0];
                            if (!seasonMek?.message) return;

                            const seasonChoice = seasonMek.message.conversation || seasonMek.message.extendedTextMessage?.text;
                            const isReplyToSeasonMsg = seasonMek.message.extendedTextMessage?.contextInfo?.stanzaId === seasonMsgID;

                            if (isReplyToSeasonMsg && sender === seasonMek.key.remoteJid) {
                                const seasonNum = parseInt(seasonChoice) - 1;

                                if (isNaN(seasonNum) || seasonNum < 0 || seasonNum >= seasonsArray.length) {
                                    await socket.sendMessage(sender, {
                                        image:  { url: pickBotImage(sessionConfig) },
                                        caption: formatMessage(
                                            '❌ INVALID SELECTION',
                                            `*වැරදි අංකයක්! 1-${seasonsArray.length} අතර තෝරන්න!*`,
                                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                        )
                                    }, { quoted: seasonMek });
                                    return;
                                }

                                const selectedSeason = seasonsArray[seasonNum];

                                let episodesText =
    `☘️ *𝗧𝗩-𝗦𝗘𝗥𝗜𝗘𝗦 : _𝗘𝗣𝗜𝗦𝗢𝗗𝗘 𝗦𝗘𝗟𝗘𝗖𝗧𝗜𝗢𝗡_* 📺
╭──────●➤
☘️ *𝗧ɪᴛʟᴇ ➟* _${tvInfo.title || 'N/A'}_
📀 *𝗦ᴇᴀꜱᴏɴ ➟* _Season ${selectedSeason.season}_
📊 *𝗧ᴏᴛᴀʟ ➟* _${selectedSeason.total_episodes} Episodes_
╰──────────●➤
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤\n\n`;

                                selectedSeason.episodes.forEach((ep, idx) => {
                                    episodesText += `*⭐${idx + 1} ║❯❯ 📺 Episode ${ep.episode}: ${ep.title}*\n`;
                                });

                                episodesText += `\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

                                const episodeMsg = await socket.sendMessage(sender, {
                                    text: episodesText
                                }, { quoted: seasonMek });

                                const episodeMsgID = episodeMsg.key.id;

                                const handleEpisodeSelect = async ({ messages: episodeMessages }) => {
                                    const episodeMek = episodeMessages[0];
                                    if (!episodeMek?.message) return;

                                    const episodeChoice = episodeMek.message.conversation || episodeMek.message.extendedTextMessage?.text;
                                    const isReplyToEpisodeMsg = episodeMek.message.extendedTextMessage?.contextInfo?.stanzaId === episodeMsgID;

                                    if (isReplyToEpisodeMsg && sender === episodeMek.key.remoteJid) {
                                        const choiceNum = parseInt(episodeChoice);

                                        if (isNaN(choiceNum) || choiceNum < 1 || choiceNum > selectedSeason.episodes.length) {
                                            await socket.sendMessage(sender, {
                                                image:  { url: pickBotImage(sessionConfig) },
                                                caption: formatMessage(
                                                    '❌ INVALID SELECTION',
                                                    `*වැරදි අංකයක්! 1-${selectedSeason.episodes.length} අතර තෝරන්න!*`,
                                                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                )
                                            }, { quoted: episodeMek });
                                            return;
                                        }

                                        const selectedEpisode = selectedSeason.episodes[choiceNum - 1];

                                        await socket.sendMessage(sender, { 
                                            text: `📥 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠𝙨 𝙛𝙤𝙧 S${selectedSeason.season}E${selectedEpisode.episode}...` 
                                        }, { quoted: episodeMek });

                                        try {
                                            // 🟢 NEW: Episode API URL
                                            const episodeResponse = await axios.get(`https://apis.laksidu.site/api/episode?url=${encodeURIComponent(selectedEpisode.url)}&api_key=lakiyaofc2`);
                                            const episodeData = episodeResponse.data;

                                            if (!episodeData.status || !episodeData.data?.download_links?.length) {
                                                throw new Error('Failed to get episode download links');
                                            }

                                            const episodeDownloadLinks = episodeData.data.download_links;

                                            let qualityText = 
    `☘️ *𝗧𝗩-𝗦𝗘𝗥𝗜𝗘𝗦 : _𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦_* 📺
╭──────●➤
🎬 *𝗧ɪᴛʟᴇ ➟* _${tvInfo.title || 'N/A'}_
📀 *𝗦ᴇᴀꜱᴏɴ ➟* _Season ${selectedSeason.season}_
📺 *𝗘ᴘɪꜱᴏᴅᴇ ➟* _${selectedEpisode.episode} : ${selectedEpisode.title}_
╰──────────●➤
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤\n\n`;

                                            episodeDownloadLinks.forEach((link, idx) => {
                                                const quality = link.meta || link.type || `Quality ${idx + 1}`;
                                                qualityText += `🔥 *${idx + 1} ║❯❯ 📥 ${quality}*\n`;
                                            });

                                            qualityText += `\n${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

                                            const qualityMsg = await socket.sendMessage(sender, {
                                                text: qualityText
                                            }, { quoted: episodeMek });

                                            const qualityMsgID = qualityMsg.key.id;

                                            const handleQualitySelect = async ({ messages: qualityMessages }) => {
                                                const qualityMek = qualityMessages[0];
                                                if (!qualityMek?.message) return;

                                                const qualityChoice = qualityMek.message.conversation || qualityMek.message.extendedTextMessage?.text;
                                                const isReplyToQualityMsg = qualityMek.message.extendedTextMessage?.contextInfo?.stanzaId === qualityMsgID;

                                                if (isReplyToQualityMsg && sender === qualityMek.key.remoteJid) {
                                                    const qualityNum = parseInt(qualityChoice) - 1;

                                                    if (isNaN(qualityNum) || qualityNum < 0 || qualityNum >= episodeDownloadLinks.length) {
                                                        await socket.sendMessage(sender, {
                                                            image:  { url: pickBotImage(sessionConfig) },
                                                            caption: formatMessage(
                                                                '❌ INVALID SELECTION',
                                                                `*වැරදි අංකයක්! 1-${episodeDownloadLinks.length} අතර තෝරන්න!*`,
                                                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                            )
                                                        }, { quoted: qualityMek });
                                                        return;
                                                    }

                                                    const selectedQuality = episodeDownloadLinks[qualityNum];

                                                    await socket.sendMessage(sender, { 
                                                        text: `⏳ 𝙂𝙚𝙩𝙩𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠...` 
                                                    }, { quoted: qualityMek });

                                                    try {
                                                        // 🟢 NEW: Download API - using selectedQuality.url (full ZT link)
                                                        let finalLink;
                                                        try {
                                                            const darkShanResponse = await cineGetWithRetry(`https://apis.laksidu.site/dl/cinesubz?url=${encodeURIComponent(selectedQuality.url)}&api_key=lakiyaofc2`);
                                                            const darkShanData = darkShanResponse.data;
                                                            if (!darkShanData.status || !darkShanData.data?.download) throw new Error('Failed to get download URL');
                                                            const finalNonTelegramLinks = darkShanData.data.download.filter(link => link.name && link.name.toLowerCase() !== 'telegram');
                                                            if (finalNonTelegramLinks.length === 0) throw new Error('No non-Telegram download links available');
                                                            finalLink = finalNonTelegramLinks.find(link => link.name === 'unknown') || finalNonTelegramLinks[0];
                                                        } catch (primaryErr) {
                                                            console.error('[cinesubz tv] primary download API failed, trying fallback:', primaryErr.message);
                                                            finalLink = await cineFallbackEpisodeLink(selectedItem.link, tvInfo.episodes?.list, selectedEpisode);
                                                        }

                                                        await socket.sendMessage(sender, { react: { text: '📥', key: qualityMek.key } });

                                                        await socket.sendMessage(targetGroupJid, {
                                                            image: { url: tvInfo.poster || pickBotImage(sessionConfig) },
                                                            caption: tvDetailsText
                                                        });
                                                        await new Promise(r => setTimeout(r, 2000));
                                                        await socket.sendMessage(targetGroupJid, {
                                                            document: { url: finalLink.url },
                                                            mimetype: 'video/mp4',
                                                            fileName: `${tvInfo.title || 'Series'} S${selectedSeason.season}E${selectedEpisode.episode} - ${selectedEpisode.title}.mp4`,
                                                            caption: `*☘️ ${tvInfo.title || 'Series'} - ${selectedSeason.season}*

\`[Episode-${selectedEpisode.episode}]\`

${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                                                        });
                                                        await socket.sendMessage(sender, { text: `✅ *${tvInfo.title || 'Series'}* S${selectedSeason.season}E${selectedEpisode.episode} group ekata yawwa!\n👥 ${targetGroupName}` }, { quoted: qualityMek });

                                                        await socket.sendMessage(sender, { react: { text: '✅', key: qualityMek.key } });

                                                    } catch (downloadError) {
                                                        console.error('Download error:', downloadError);
                                                        await socket.sendMessage(sender, {
                                                            image:  { url: pickBotImage(sessionConfig) },
                                                            caption: formatMessage(
                                                                '❌ DOWNLOAD ERROR',
                                                                `*Download link එක ලබාගැනීමේ දෝෂයක්.*\n${downloadError.message}`,
                                                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                            )
                                                        }, { quoted: qualityMek });
                                                    } finally {
                                                        socket.ev.off('messages.upsert', handleQualitySelect);
                                                        socket.ev.off('messages.upsert', handleEpisodeSelect);
                                                        socket.ev.off('messages.upsert', handleSeasonSelect);
                                                        socket.ev.off('messages.upsert', handleSelection);
                                                    }
                                                }
                                            };

                                            socket.ev.on('messages.upsert', handleQualitySelect);

                                        } catch (error) {
                                            console.error('Error fetching episode links:', error);
                                            await socket.sendMessage(sender, {
                                                image:  { url: pickBotImage(sessionConfig) },
                                                caption: formatMessage(
                                                    '❌ ERROR',
                                                    `*Download links ලබාගැනීමේ දෝෂයක්*\n${error.message}`,
                                                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                )
                                            }, { quoted: episodeMek });
                                            socket.ev.off('messages.upsert', handleEpisodeSelect);
                                            socket.ev.off('messages.upsert', handleSeasonSelect);
                                            socket.ev.off('messages.upsert', handleSelection);
                                        }
                                    }
                                };

                                socket.ev.on('messages.upsert', handleEpisodeSelect);
                            }
                        };

                        socket.ev.on('messages.upsert', handleSeasonSelect);

                    } catch (tvShowError) {
                        console.error('TV Show error:', tvShowError);
                        await socket.sendMessage(sender, {
                            image:  { url: pickBotImage(sessionConfig) },
                            caption: formatMessage(
                                '❌ ERROR',
                                `*TV series details ලබාගැනීමේ දෝෂයක්*\n${tvShowError.message}`,
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }

                } else {
                    await socket.sendMessage(sender, { 
                        text: '📽️ 𝙁𝙚𝙩????𝙞𝙣𝙜 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' 
                    }, { quoted: replyMek });

                    try {
                        const detailsResponse = await axios.get(`https://apis.laksidu.site/cinesubz/details?url=${encodeURIComponent(selectedItem.link)}&api_key=lakiyaofc2`);
                        const detailsData = detailsResponse.data;

                        if (!detailsData.status || !detailsData.data) {
                            throw new Error('Failed to fetch details');
                        }

                        const movieInfo = detailsData.data;

                        const validDownloads = movieInfo.downloads?.filter(dl => dl && dl.quality && dl.url) || [];

                        if (validDownloads.length === 0) {
                            await socket.sendMessage(sender, {
                                image:  { url: pickBotImage(sessionConfig) },
                                caption: formatMessage(
                                    '❌ NO DOWNLOADS',
                                    '*මෙම චිත්‍රපටය සඳහා බාගත කිරීමේ link නොමැත!*',
                                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                )
                            }, { quoted: replyMek });
                            return;
                        }

                        const description = movieInfo.description?.substring(0, 300) + (movieInfo.description?.length > 300 ? '...' : '') || 'No description available.';

                        const imdbRating = movieInfo.imdb_rating ? `${movieInfo.imdb_rating}/10` : 'N/A';
                        const year = movieInfo.year || 'N/A';
                        const runtime = movieInfo.runtime || 'N/A';
                        const director = movieInfo.director || 'N/A';
                        const country = movieInfo.country || 'N/A';
                        const cast = Array.isArray(movieInfo.cast) ? movieInfo.cast.join(', ') : movieInfo.cast || 'N/A';

                        const movieDetailsCaption = formatMessage(
                            `☘️ *𝗧ɪᴛʟᴇ ➟* _${movieInfo.title}_`,
                            `▫️🥇 *𝗜𝗺𝗱𝗯 𝗥ᴀᴛɪɴɢ ➟* _${imdbRating}_
▫️⏳ *𝗗ᴜʀᴀᴛɪᴏɴ ➟* _${runtime}_
▫️📅 *𝗥ᴇʟᴇᴀꜱᴇ 𝗬ᴇᴀʀ ➟* _${year}_
▫️🎬 *𝗗ɪʀᴇᴄᴛᴏʀ ➟* _${director}_
▫️🌎 *𝗖ᴏᴜɴᴛʀʏ ➟* _${country}_
▫️👥 *𝗖ᴀꜱᴛ ➟* _${cast}_
*➟➟➟➟➟➟➟➟➟➟*
*📖 𝗦𝗧𝗢𝗥𝗬 ➟*_${description}_`,
                            `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                        );

                        await socket.sendMessage(sender, {
                            image: { url: movieInfo.poster || sessionConfig.LAKIYA_IMAGE_PATH || config.LAKIYA_IMAGE_PATH },
                            caption: movieDetailsCaption
                        }, { quoted: replyMek });

                        const downloadOptionsCaption = formatMessage(
                            `⬇️🍀 *𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦*`,
                            `${validDownloads.map((dl, i) => `▫️ *${(i + 1).toString().padStart(2, '0')} ❱❱ 📥 ${dl.quality}*`).join('\n')}\n

╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        );

                        const downloadOptionsMsg = await socket.sendMessage(sender, {
                            text: downloadOptionsCaption
                        }, { quoted: replyMek });

                        const optionsMsgID = downloadOptionsMsg.key.id;

                        const handleDownload = async ({ messages: downloadMessages }) => {
                            const downloadMek = downloadMessages[0];
                            if (!downloadMek?.message) return;

                            const downloadChoice = downloadMek.message.conversation || downloadMek.message.extendedTextMessage?.text;
                            const isReplyToOptionsMsg = downloadMek.message.extendedTextMessage?.contextInfo?.stanzaId === optionsMsgID;

                            if (isReplyToOptionsMsg && sender === downloadMek.key.remoteJid) {
                                const choiceNum = parseInt(downloadChoice) - 1;

                                if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= validDownloads.length) {
                                    await socket.sendMessage(sender, {
                                        image:  { url: pickBotImage(sessionConfig) },
                                        caption: formatMessage(
                                            '❌ INVALID SELECTION',
                                            `*වැරදි අංකයක්! 1-${validDownloads.length} අතර තෝරන්න!*`,
                                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                        )
                                    }, { quoted: downloadMek });
                                    return;
                                }

                                const selectedDownload = validDownloads[choiceNum];

                                await socket.sendMessage(sender, { 
                                    text: `⏳ 𝙂𝙚𝙩𝙩𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠...` 
                                }, { quoted: downloadMek });

                                try {
                                    let downloadData, preferredLink;
                                    try {
                                        const downloadResponse = await cineGetWithRetry(`https://apis.laksidu.site/dl/cinesubz?url=${encodeURIComponent(selectedDownload.url)}&api_key=lakiyaofc2`);
                                        downloadData = downloadResponse.data;
                                        if (!downloadData.status || !downloadData.data?.download) throw new Error('Failed to get download URL');
                                        const nonTelegramLinks = downloadData.data.download.filter(link => link.name && link.name.toLowerCase() !== 'telegram');
                                        if (nonTelegramLinks.length === 0) throw new Error('No non-Telegram download links available');
                                        preferredLink = nonTelegramLinks.find(link => link.name === 'unknown') || nonTelegramLinks[0];
                                    } catch (primaryErr) {
                                        console.error('[cinesubz] primary download API failed, trying fallback:', primaryErr.message);
                                        const fb = await cineFallbackMovieLink(selectedItem.link, selectedDownload.quality, choiceNum);
                                        downloadData = { data: {} };
                                        preferredLink = { url: fb.url };
                                    }

                                    await socket.sendMessage(sender, { react: { text: '📥', key: downloadMek.key } });

                                    await socket.sendMessage(targetGroupJid, {
                                        image: { url: movieInfo.poster || pickBotImage(sessionConfig) },
                                        caption: movieDetailsCaption
                                    });
                                    await new Promise(r => setTimeout(r, 2000));
                                    await socket.sendMessage(targetGroupJid, {
                                        document: { url: preferredLink.url },
                                        mimetype: 'video/mp4',
                                        fileName: downloadData.data.title || `${movieInfo.title} ${selectedDownload.quality}.mp4`,
                                        caption: formatMessage(
                                            `☘️ ${movieInfo.title}`,
                                            `\`❚█═${sessionConfig.MOVIE_CAPTION || config.MOVIE_CAPTION}═█❚\`
                                            
\`[WEB-DL-${selectedDownload.quality}]\``,
                                            `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                                        )
                                    });
                                    await socket.sendMessage(sender, { text: `✅ *${movieInfo.title}* (${selectedDownload.quality}) group ekata yawwa!\n👥 ${targetGroupName}` }, { quoted: downloadMek });

                                    await socket.sendMessage(sender, { react: { text: '✅', key: downloadMek.key } });

                                } catch (downloadError) {
                                    console.error('Download link error:', downloadError);
                                    await socket.sendMessage(sender, {
                                        image:  { url: pickBotImage(sessionConfig) },
                                        caption: formatMessage(
                                            '❌ DOWNLOAD ERROR',
                                            `*Download link එක ලබාගැනීමේ දෝෂයක්.*\n${downloadError.message}`,
                                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                        )
                                    }, { quoted: downloadMek });
                                } finally {
                                    socket.ev.off('messages.upsert', handleDownload);
                                    socket.ev.off('messages.upsert', handleSelection);
                                }
                            }
                        };

                        socket.ev.on('messages.upsert', handleDownload);

                    } catch (detailsError) {
                        console.error('Details error:', detailsError);
                        await socket.sendMessage(sender, {
                            image:  { url: pickBotImage(sessionConfig) },
                            caption: formatMessage(
                                '❌ ERROR',
                                `*Details ලබාගැනීමේ දෝෂයක්*\n${detailsError.message}`,
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }
                }
            }
        };

        socket.ev.on('messages.upsert', handleSelection);

    } catch (error) {
        console.error('Cinezub command error:', error);
        await socket.sendMessage(sender, {
            image:  { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                `*දෝෂයක් ඇතිවුණා:* ${error.message || 'Unknown error'}`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }

    break;
}

case 'tvsend': {
    const DEFAULT_FOOTER = `\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗠𝗢𝗩𝗜𝗘 𝗕𝗢𝗧 🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ ʟYɴᴋᴏ`;

    if (!isOwner) {
        await socket.sendMessage(sender, { text: '❌ *Owner witharai meka use karanna puluwan.*' }, { quoted: msg });
        break;
    }
    const gsArgs = args.join(' ');
    const gsComma = gsArgs.indexOf(',');
    const gsLinkInput = gsComma === -1 ? '' : gsArgs.slice(0, gsComma).trim();
    const gsQuery = gsComma === -1 ? '' : gsArgs.slice(gsComma + 1).trim();
    if (!gsLinkInput || !gsQuery) {
        await socket.sendMessage(sender, {
            text: `❌ *Use karana widiya:*\n\n\`${sessionConfig.PREFIX || config.PREFIX || '.'}tvsend <group link>, <series / movie name>\`\n\n*Example:*\n${sessionConfig.PREFIX || config.PREFIX || '.'}tvsend https://chat.whatsapp.com/xxxxx, game of thrones\n\n_Bot number eka e group ekata add wela inna one._`
        }, { quoted: msg });
        break;
    }
    let targetGroupJid = null;
    let targetGroupName = 'Group';
    try {
        const gsCode = gsLinkInput.includes('chat.whatsapp.com/') ? gsLinkInput.split('chat.whatsapp.com/')[1].split('?')[0].trim() : gsLinkInput;
        const gsInfo = await socket.groupGetInviteInfo(gsCode);
        targetGroupJid = gsInfo.id;
        targetGroupName = gsInfo.subject || targetGroupName;
        await socket.groupMetadata(targetGroupJid); // bot eka e group eke inna one
    } catch (gsErr) {
        await socket.sendMessage(sender, {
            text: `❌ *Group ekata send karanna baha.*\n\n_Link eka waradi, ho bot eka e group ekata add wela na._\n${gsErr.message}`
        }, { quoted: msg });
        break;
    }
    await socket.sendMessage(sender, { text: `📤 *Send to :* ${targetGroupName}` }, { quoted: msg });

    const cinesubQuery = gsQuery;
    await socket.sendMessage(sender, { 
        text: `*❪ SEARCHING ❫*\n\n🔍 *Searching Movies...*\n⚡ _Please wait a moment._`
    });

    const API_BASE = "https://api.chamindu.site";
    const API_KEY = "chama_api_11230a80e5eed3c1b80bfcc5d1773ec9"; // ඔබේ API Key එක දාන්න
    const DEFAULT_IMAGE = "https://api.chamindu.site/logo.png";

    try {
        const searchResponse = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/search?q=${encodeURIComponent(cinesubQuery)}&api_key=${API_KEY}`);
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                text: `*❪ NO RESULTS ❫*\n\n😞 *No Results Found!*\n\n🎬 *Query:* _${cinesubQuery}_\n💡 *Tip:* _Please check the spelling and try again!_${DEFAULT_FOOTER}`
            }, { quoted: msg });
            break;
        }

        const cinesubResults = searchData.data.slice(0, 25);
        let listText = `*❪ SEARCH RESULTS ❫*\n\n🎯 *Query:* _${cinesubQuery}_\n📊 *Results:* _${cinesubResults.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;

        cinesubResults.forEach((item, index) => {
            const typeIcon = item.type === 'tvshows' ? '📺' : '🎥';
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            listText += `*${num}* ➜ ${typeIcon} _${item.title.substring(0, 30)}_\n`;
        });

        listText += `${DEFAULT_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, { text: listText }, { quoted: msg });
        const messageID = sentMsg.key.id;

        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= cinesubResults.length) {
                    await socket.sendMessage(sender, {
                        text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 - ${cinesubResults.length}_\n📝 _Please reply with a valid number!_${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                    return;
                }

                const selectedItem = cinesubResults[choice];
                const isTvShow = selectedItem.type === 'tvshows';

                if (isTvShow) {
                    await socket.sendMessage(sender, { 
                        text: `*❪ FETCHING ❫*\n\n📺 *Fetching TV Series...*\n⚡ _Please wait..._`
                    }, { quoted: replyMek });

                    try {
                        const tvShowResponse = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/tv/info?q=${encodeURIComponent(selectedItem.link)}&api_key=${API_KEY}`);
                        const tvShowData = tvShowResponse.data;

                        if (!tvShowData.status || !tvShowData.data) {
                            throw new Error('Failed to fetch TV show details');
                        }

                        const tvInfo = tvShowData.data;

                        let tvDetailsText = `*❪ TV SERIES DETAILS ❫*\n\n📺 *${tvInfo.title}*\n⭐ 𝗜ᴍᴅʙ ➜ ★ ${tvInfo.rating || 'N/A'}\n📅 𝗬ᴇᴀʀ ➜ ${tvInfo.year || 'N/A'}\n⏳ 𝗥ᴜɴᴛɪᴍᴇ ➜ ${tvInfo.duration || 'N/A'}\n🌍 𝗖ᴏᴜɴ𝘁𝗿ʏ ➜ ${tvInfo.country || 'N/A'}\n🎭 𝗚𝗲𝗻 genres ➜ ${tvInfo.genres ? tvInfo.genres.join(', ') : 'N/A'}\n🎬 𝗗ɪʀᴇᴄᴛᴏʀ ➜ ${tvInfo.directors || 'N/A'}\n⭐ 𝗦ᴛᴀʀ𝘀: ${tvInfo.stars || 'N/A'}\n📝 𝗦𝘁𝗼𝗿𝘆 ➜ ${tvInfo.story ? (tvInfo.story.length > 250 ? tvInfo.story.substring(0, 250) + '...' : tvInfo.story) : 'N/A'}\n🗿 𝗪ᴇʙ ➜ cinesubz.com\n ${DEFAULT_FOOTER}`;

                        const posterUrl = tvInfo.image || selectedItem.image || DEFAULT_IMAGE;
                        await socket.sendMessage(sender, {
                            image: { url: posterUrl },
                            caption: tvDetailsText
                        }, { quoted: replyMek });

                        // series post eka group ekata
                        await socket.sendMessage(targetGroupJid, { image: { url: posterUrl }, caption: tvDetailsText });
                        await new Promise(r => setTimeout(r, 2000));

                        // AUTO DOWNLOAD ALL EPISODES
                        await socket.sendMessage(sender, { 
                            text: `*❪ DOWNLOAD EPISODES ❫*\n\n📺 *Series:* _${tvInfo.title}_\n🎬 *Episodes:* _${tvInfo.episodes.length}_\n⚡ _Starting download process..._${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });

                        let successCount = 0;
                        let failCount = 0;

                        for (let i = 0; i < tvInfo.episodes.length; i++) {
                            const episode = tvInfo.episodes[i];
                            try {
                                await socket.sendMessage(sender, { 
                                    text: `*❪ DOWNLOADING ❫*\n\n🎥 *Episode:* _${episode.episode_name}_\n📊 *Progress:* _${i + 1}/${tvInfo.episodes.length}_`
                                }, { quoted: replyMek });

                                const epDlRes = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/tv/dl?q=${encodeURIComponent(episode.episode_url)}&api_key=${API_KEY}`);
                                const epDlData = epDlRes.data;

                                if (epDlData.status && epDlData.data && epDlData.data.length > 0) {
                                    const nonTelegramLinks = epDlData.data.filter(link => 
                                        link.link && !link.link.includes('t.me') && !link.link.includes('telegram')
                                    );
                                    const finalLinkObj = nonTelegramLinks[0] || epDlData.data[0];

                                    await socket.sendMessage(targetGroupJid, {
                                        document: { url: finalLinkObj.link },
                                        mimetype: 'video/mp4',
                                        fileName: `${tvInfo.title} - ${episode.episode_name}.mp4`,
                                        caption: `*📺 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗠𝗢𝗩𝗜𝗘 𝗕𝗢𝗧 📺*\n\n🎭 *Title:* ${tvInfo.title}\n📌 *Episode:* ${episode.episode_name}\n📊 *Quality:* Direct MP4\n\n${DEFAULT_FOOTER}`
                                    });

                                    successCount++;
                                } else {
                                    failCount++;
                                }

                                await new Promise(resolve => setTimeout(resolve, 2500));

                            } catch (epError) {
                                console.error(`Error downloading episode:`, epError);
                                failCount++;
                            }
                        }

                        await socket.sendMessage(sender, { 
                            text: `*❪ SUMMARY ❫*\n\n📤 *Group:* _${targetGroupName}_\n\n🎉 *Download Complete!*\n\n🎬 *Series:* _${tvInfo.title}_\n✅ *Success:* _${successCount} Episodes_\n❌ *Failed:* _${failCount} Episodes_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });

                        socket.ev.off('messages.upsert', handleSelection);

                    } catch (tvShowError) {
                        console.error('TV Show error:', tvShowError);
                        await socket.sendMessage(sender, {
                            text: `*❪ ERROR ❫*\n\n❌ *TV Details Error!*\n🚫 _${tvShowError.message}_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }

                } else {
                    // MOVIE FLOW
                    await socket.sendMessage(sender, { 
                        text: `*❪ FETCHING ❫*\n\n🎬 *Fetching Movie...*\n⚡ _Please wait..._`
                    }, { quoted: replyMek });

                    try {
                        const detailsResponse = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/infodl?q=${encodeURIComponent(selectedItem.link)}&api_key=${API_KEY}`);
                        const detailsData = detailsResponse.data;

                        if (!detailsData.status || !detailsData.data) {
                            throw new Error('Failed to fetch details');
                        }

                        const movieInfo = detailsData.data;
                        const validDownloads = movieInfo.downloads || [];

                        if (validDownloads.length === 0) {
                            await socket.sendMessage(sender, {
                                text: `*❪ NO DOWNLOADS ❫*\n\n⚠️ *No Downloads Found!*\n😞 _There are no downloads available for this movie!_${DEFAULT_FOOTER}`
                            }, { quoted: replyMek });
                            return;
                        }

                        const movieDetailsText = `*❪ MOVIE DETAILS ❫*\n\n🎬 *${movieInfo.title}*\n⭐ 𝗜𝗠𝗗𝗕 ➜ ★ ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n📅 𝗬𝗲𝗮𝗿 ➜ ${movieInfo.year || 'N/A'}\n⏳ 𝗗𝘂𝗿𝗮𝘁𝗶𝗼𝗻 ➜ ${movieInfo.duration || 'N/A'}\n🌍 𝗖ᴏᴜɴ𝘁𝗿ʏ ➜ ${movieInfo.country || 'N/A'}\n🎭 𝗚𝗲𝗻 genres ➜ ${movieInfo.genres ? movieInfo.genres.join(', ') : 'N/A'}\n🏷️  ➜ ${movieInfo.language || movieInfo.tag || 'N/A'}\n🎬  ➜ ${movieInfo.directors || movieInfo.director || 'N/A'}\n⭐  ➜ ${movieInfo.stars || 'N/A'}\n📝  ➜ ${movieInfo.story ? (movieInfo.story.length > 250 ? movieInfo.story.substring(0, 250) + '...' : movieInfo.story) : 'N/A'}\n🗿 𝗪ᴇʙ ➜ cinesubz.com\n ${DEFAULT_FOOTER}`;

                        const moviePosterUrl = movieInfo.image || selectedItem.image || DEFAULT_IMAGE;
                        await socket.sendMessage(sender, {
                            image: { url: moviePosterUrl },
                            caption: movieDetailsText
                        }, { quoted: replyMek });

                        const downloadOptionsText = `*❪ DOWNLOADS ❫*\n\n📥 *Select Quality:*\n\n${validDownloads.map((dl, i) => {
    const num = (i + 1) < 10 ? `0${i + 1}` : `${i + 1}`;
    const qualityIcon = (dl.quality || '').includes('1080') ? '🔥' : (dl.quality || '').includes('720') ? '💎' : '📱';
    return `*${num}* ➜ ${qualityIcon} _${dl.quality}_ 💾 _${dl.size || 'N/A'}_`;
}).join('\n')}\n\n*💬 REPLY TO DOWNLOAD 💬*\n📌 _Reply with the number_${DEFAULT_FOOTER}`;

                        const downloadOptionsMsg = await socket.sendMessage(sender, { text: downloadOptionsText }, { quoted: replyMek });
                        const optionsMsgID = downloadOptionsMsg.key.id;

                        const handleDownload = async ({ messages: downloadMessages }) => {
                            const downloadMek = downloadMessages[0];
                            if (!downloadMek?.message) return;

                            const downloadChoice = downloadMek.message.conversation || downloadMek.message.extendedTextMessage?.text;
                            const isReplyToOptionsMsg = downloadMek.message.extendedTextMessage?.contextInfo?.stanzaId === optionsMsgID;

                            if (isReplyToOptionsMsg && sender === downloadMek.key.remoteJid) {
                                const choiceNum = parseInt(downloadChoice) - 1;

                                if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= validDownloads.length) {
                                    await socket.sendMessage(sender, {
                                        text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 - ${validDownloads.length}_\n📝 _Please reply with a valid number!_${DEFAULT_FOOTER}`
                                    }, { quoted: downloadMek });
                                    return;
                                }

                                const selectedDownload = validDownloads[choiceNum];
                                await socket.sendMessage(sender, { react: { text: '📥', key: downloadMek.key } });

                                try {
                                    const finalDirectLink = selectedDownload.link;

                                    await socket.sendMessage(targetGroupJid, { image: { url: moviePosterUrl }, caption: movieDetailsText });
                                    await new Promise(r => setTimeout(r, 2000));
                                    await socket.sendMessage(targetGroupJid, {
                                        document: { url: finalDirectLink },
                                        mimetype: 'video/mp4',
                                        fileName: `${movieInfo.title} - ${selectedDownload.quality}.mp4`,
                                        caption: `*🎬 𝗦𝗛𝗔𝗚𝗚𝗬 𝗠𝗢𝗩𝗜𝗘 🎬*\n\n🎭 *Title:* ${movieInfo.title}\n🌟 *IMDB:* ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n📅 *Year:* ${movieInfo.year || 'N/A'}\n📊 *Quality:* ${selectedDownload.quality}\n💾 *Size:* ${selectedDownload.size || 'N/A'}\n\n${DEFAULT_FOOTER}`
                                    });
                                    await socket.sendMessage(sender, { text: `✅ *${movieInfo.title}* (${selectedDownload.quality}) group ekata yawwa!\n👥 ${targetGroupName}` }, { quoted: downloadMek });

                                    await socket.sendMessage(sender, { react: { text: '✅', key: downloadMek.key } });

                                } catch (downloadError) {
                                    console.error('Download link error:', downloadError);
                                    await socket.sendMessage(sender, {
                                        text: `*❪ ERROR ❫*\n\n❌ *Download Failed!*\n🚫 _${downloadError.message}_${DEFAULT_FOOTER}`
                                    }, { quoted: downloadMek });
                                } finally {
                                    socket.ev.off('messages.upsert', handleDownload);
                                    socket.ev.off('messages.upsert', handleSelection);
                                }
                            }
                        };

                        socket.ev.on('messages.upsert', handleDownload);

                    } catch (detailsError) {
                        console.error('Details error:', detailsError);
                        await socket.sendMessage(sender, {
                            text: `*❪ ERROR ❫*\n\n❌ *Movie Details Error!*\n🚫 _${detailsError.message}_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }
                }
            }
        };

        socket.ev.on('messages.upsert', handleSelection);

    } catch (error) {
        console.error('Cinesubz command error:', error);
        await socket.sendMessage(sender, {
            text: `*❪ SYSTEM ERROR ❫*\n\n❌ *System Error!*\n🚫 _${error.message || 'Unknown error'}_\n\n🔄 _Please try again later..._${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }

    break;
}

case 'singrup':
    if (!args.length || !args.join(' ').includes(',')) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර group link සහ චිත්‍රපටයේ නම ලබාදෙන්න!*\n*උදා: .singrup https://chat.whatsapp.com/xxxxx,spider*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    // Parse gruplink and moviename
    const singrupArgs = args.join(' ');
    const commaIndex = singrupArgs.indexOf(',');
    const groupLinkInput = singrupArgs.substring(0, commaIndex).trim();
    const movieQuery = singrupArgs.substring(commaIndex + 1).trim();

    if (!groupLinkInput || !movieQuery) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*Group link සහ movie name දෙකම ලබාදෙන්න!*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    // Extract group invite code
    let groupInviteCode = groupLinkInput;
    if (groupLinkInput.includes('chat.whatsapp.com/')) {
        groupInviteCode = groupLinkInput.split('chat.whatsapp.com/')[1].split('?')[0].trim();
    }

    // Get target group JID
    let targetGroupJid = null;
    let targetGroupName = 'Target Group';
    try {
        const groupInfo = await socket.groupGetInviteInfo(groupInviteCode);
        targetGroupJid = groupInfo.id;
        targetGroupName = groupInfo.subject || targetGroupName;
        console.log('✅ Target group JID:', targetGroupJid);
    } catch (groupErr) {
        console.error('Group link error:', groupErr);
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ GROUP ERROR',
                `*Group link එක වැරදියි හෝ bot ට access නැහැ!*\nError: ${groupErr.message}`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

    let singrupSelectionListener = null;
    let singrupDownloadListener = null;
    let singrupSelectionTimeout = null;
    let singrupDownloadTimeout = null;
    let singrupMasterTimeout = null;

    const clearAllSingrupListeners = () => {
        console.log('🧹 Clearing all Singrup listeners');
        if (singrupSelectionListener) {
            socket.ev.off('messages.upsert', singrupSelectionListener);
            singrupSelectionListener = null;
        }
        if (singrupSelectionTimeout) {
            clearTimeout(singrupSelectionTimeout);
            singrupSelectionTimeout = null;
        }
        if (singrupDownloadListener) {
            socket.ev.off('messages.upsert', singrupDownloadListener);
            singrupDownloadListener = null;
        }
        if (singrupDownloadTimeout) {
            clearTimeout(singrupDownloadTimeout);
            singrupDownloadTimeout = null;
        }
        if (singrupMasterTimeout) {
            clearTimeout(singrupMasterTimeout);
            singrupMasterTimeout = null;
        }
    };

    try {
        const searchResponse = await axios.get(`${config.API_MAIN_URL}/sinhalasub/search?query=${encodeURIComponent(movieQuery)}&api_key=${config.API_KEY}`);
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.data?.results || searchData.data.results.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*චිත්‍රපට හමුවෙන්නේ නැත! 😞*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const movies = searchData.data.results.slice(0, 115);
        let listText = `🎀 *𝗦𝗘𝗔𝗥𝗖𝗛 : _${movieQuery}_*
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤
╭──────●➤\n`;

        movies.forEach((movie, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${movie.title}*\n`;
        });

        listText += `╰──────────●➤\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const messageID = sentMsg.key.id;

        singrupMasterTimeout = setTimeout(() => {
            clearAllSingrupListeners();
            console.log('🧹 Singrup master timeout');
        }, 180000);

        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                if (singrupSelectionTimeout) {
                    clearTimeout(singrupSelectionTimeout);
                    singrupSelectionTimeout = null;
                }

                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= movies.length) {
                    await socket.sendMessage(sender, {
                        image: { url: pickBotImage(sessionConfig) },
                        caption: formatMessage(
                            '❌ INVALID SELECTION',
                            `*වැරදි අංකයක්! 1-${movies.length} අතර තෝරන්න! 😕*`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                    return;
                }

                const selectedMovie = movies[choice];

                await socket.sendMessage(sender, {
                    text: '📽️ 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...'
                }, { quoted: replyMek });

                await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

                try {
                    const infoResponse = await axios.get(`${config.API_MAIN_URL}/sinhalasub/info?url=${encodeURIComponent(selectedMovie.url)}&api_key=${config.API_KEY}`);
                    const infoData = infoResponse.data;

                    if (!infoData.status || !infoData.data) {
                        throw new Error('Failed to fetch movie details');
                    }

                    const movieInfo = infoData.data.movie;
                    const downloads = infoData.data.downloads || [];
                    const videoDownloads = downloads.filter(d => d.server === 'pixeldrain');

                    if (videoDownloads.length === 0) {
                        await socket.sendMessage(sender, {
                            image: { url: pickBotImage(sessionConfig) },
                            caption: formatMessage(
                                '❌ NO DOWNLOADS',
                                '*Pixeldrain බාගත කිරීම් නොමැත!*',
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: replyMek });
                        return;
                    }

                    const castPreview = movieInfo.cast?.slice(0, 5).join(', ') + (movieInfo.cast?.length > 5 ? '...' : '');

                    const detailsCaption = formatMessage(
                        `🍀 *𝗧ɪᴛʟᴇ : ${movieInfo.title}`,
                        `▫️📅 *𝗥ᴇʟᴇᴀꜱᴇ 𝗬ᴇᴀʀ ➟ ${movieInfo.year || 'N/A'}*
▫️🥇 *𝗜𝗺𝗱ʙ 𝗥ᴀᴛɪɴɢ ➟ ${movieInfo.rating || 'N/A'}/10*
▫️📊 *𝗤ᴜᴀʟɪᴛʏ ➟ ${movieInfo.quality || 'N/A'}*
▫️⏳ *𝗗ᴜʀᴀᴛɪᴏɴ ➟ ${movieInfo.runtime || 'N/A'}*
▫️🔠 *𝗟ᴀɴɢᴜᴀɢᴇ ➟ ${movieInfo.language || 'N/A'}*
▫️🎭 *𝗚ᴇɴʀᴇꜱ ➟ ${movieInfo.genres?.join(', ') || 'N/A'}*
▫️🙅 *𝗗ɪʀᴇᴄᴛᴏʀ ➟ ${movieInfo.director?.slice(0,2).join(', ') || 'N/A'}*
▫️👥 *𝗖ᴀꜱᴛ ➟ ${castPreview || 'N/A'}*
▫️👨‍💻 *𝗦ᴜʙᴛɪᴛʟᴇ ➟ ${movieInfo.subtitle?.author || 'Sinhala'} (${movieInfo.subtitle?.site || 'Baiscope'})*
▫️📖 *sᴛᴏʀʏ ➟ ${movieInfo.description?.substring(0, 150) || 'No description'}...*
▫️🔗 *Jᴏɪɴ ➟ ${sessionConfig.MGROUP_LINK || config.MGROUP_LINK}*`,
                        `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                    );

                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: movieInfo.poster || selectedMovie.poster || pickBotImage(sessionConfig) },
                        caption: detailsCaption
                    }, { quoted: replyMek });

                    await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

                    const downloadOptionsText = `*⬇️🎀 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦*
*Reply with number 👇*

${videoDownloads.map((d, i) => 
`*🔰 ${i + 1} ┃ 📥 ${d.quality || 'N/A'} • ${d.size || 'N/A'}*`
).join('\n')}

${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

                    const downloadMsg = await socket.sendMessage(sender, {
                        text: downloadOptionsText
                    }, { quoted: infoMsg });

                    const infoMsgID = downloadMsg.key.id;

                    const handleDownload = async ({ messages: downloadMessages }) => {
                        const downloadMek = downloadMessages[0];
                        if (!downloadMek?.message) return;

                        const downloadChoice = downloadMek.message.conversation || downloadMek.message.extendedTextMessage?.text;
                        const isReplyToInfoMsg = downloadMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;

                        if (isReplyToInfoMsg && sender === downloadMek.key.remoteJid) {
                            if (singrupDownloadTimeout) {
                                clearTimeout(singrupDownloadTimeout);
                                singrupDownloadTimeout = null;
                            }

                            const choiceNum = parseInt(downloadChoice) - 1;

                            if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= videoDownloads.length) {
                                await socket.sendMessage(sender, {
                                    image: { url: pickBotImage(sessionConfig) },
                                    caption: formatMessage(
                                        '❌ INVALID SELECTION',
                                        `*වැරදි අංකයක්! 1-${videoDownloads.length} අතර තෝරන්න!*`,
                                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                    )
                                }, { quoted: downloadMek });
                                return;
                            }

                            const selectedDownload = videoDownloads[choiceNum];

                            await socket.sendMessage(sender, {
                                text: `⏳ 𝙂𝙚𝙩𝙩𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠...`
                            }, { quoted: downloadMek });

                            await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

                            try {
                                const downloadResponse = await axios.get(`${config.API_MAIN_URL}/sinhalasub/download2?url=${encodeURIComponent(selectedDownload.link_page)}&api_key=${config.API_KEY}`);
                                const downloadData = downloadResponse.data;

                                if (!downloadData.status || !downloadData.data?.download) {
                                    throw new Error('Failed to get download URL');
                                }

                                const finalDownloadUrl = downloadData.data.download;
                                const fileInfo = downloadData.data.file_info || {};

                                let fileName = fileInfo.name || `${movieInfo.title} [${selectedDownload.quality || 'Unknown'}].mp4`;
                                const mimeType = fileInfo.mimeType || 'video/mp4';

                                await socket.sendMessage(sender, { react: { text: '📥', key: downloadMek.key } });

                                // ============ SEND TO TARGET GROUP ============
                                const groupCaption = formatMessage(
                                    `🍀 ${movieInfo.title}`,
                                    `\`❚█${sessionConfig.MOVIE_CAPTION || config.MOVIE_CAPTION}█❚\`

\`❪${selectedDownload.quality || 'Unknown'}❫\``,
                                    `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                                );

                                // 1. Send movie post (image + details) to group
                                await socket.sendMessage(targetGroupJid, {
                                    image: { url: movieInfo.poster || selectedMovie.poster || pickBotImage(sessionConfig) },
                                    caption: detailsCaption
                                });

                                await new Promise(resolve => setTimeout(resolve, 2000));

                                // 2. Send MP4 document to group
                                await socket.sendMessage(targetGroupJid, {
                                    document: { url: finalDownloadUrl },
                                    mimetype: mimeType,
                                    fileName: fileName,
                                    caption: groupCaption
                                });

                                // 3. Confirm to user
                                await socket.sendMessage(sender, { react: { text: '✅', key: downloadMek.key } });
                                await socket.sendMessage(sender, {
                                    text: `✅ *සාර්ථකව group එකට යවන ලදී!*\n\n📽️ *${movieInfo.title}*\n📊 *${selectedDownload.quality || 'Unknown'}*\n👥 *Group:* ${targetGroupName}`
                                }, { quoted: downloadMek });

                                clearAllSingrupListeners();

                            } catch (downloadError) {
                                console.error('Download link error:', downloadError);
                                await socket.sendMessage(sender, {
                                    image: { url: pickBotImage(sessionConfig) },
                                    caption: formatMessage(
                                        '❌ DOWNLOAD ERROR',
                                        `*Download link එක ලබාගැනීමේ දෝෂයක්.*\nError: ${downloadError.message}`,
                                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                    )
                                }, { quoted: downloadMek });
                            }
                        }
                    };

                    singrupDownloadListener = handleDownload;
                    socket.ev.on('messages.upsert', handleDownload);

                    singrupDownloadTimeout = setTimeout(() => {
                        if (singrupDownloadListener) {
                            socket.ev.off('messages.upsert', singrupDownloadListener);
                            singrupDownloadListener = null;
                            console.log('🧹 Singrup download listener timeout');
                        }
                        singrupDownloadTimeout = null;
                    }, 120000);

                } catch (infoError) {
                    console.error('Movie info error:', infoError);
                    await socket.sendMessage(sender, {
                        image: { url: pickBotImage(sessionConfig) },
                        caption: formatMessage(
                            '❌ ERROR',
                            `*Movie details ලබාගැනීමේ දෝෂයක්:* ${infoError.message}`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                }
            }
        };

        singrupSelectionListener = handleSelection;
        socket.ev.on('messages.upsert', handleSelection);

        singrupSelectionTimeout = setTimeout(() => {
            if (singrupSelectionListener) {
                socket.ev.off('messages.upsert', singrupSelectionListener);
                singrupSelectionListener = null;
                console.log('🧹 Singrup selection listener timeout');
            }
            singrupSelectionTimeout = null;
        }, 120000);

    } catch (error) {
        console.error('Movie command error:', error);
        clearAllSingrupListeners();
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                `*දෝෂයක් ඇතිවුණා:* ${error.message || 'Unknown error'}`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }
    break;

case 'papers':
case 'paper':
case 'pastpapers': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර සොයන Paper එකේ නම ලබාදෙන්න! උදා: .papers Mathematics*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const query = args.join(' ').trim();
    const API_BASE = 'https://bookapi.lovable.app/api/public/v1';
    const API_KEY = 'sk_live_826111a21aa4757fb73f18fe8a24e4a06591d0e60205ea62';

    let paperListener = null;
    let paperMasterTimeout = null;

    const clearPaperListeners = () => {
        if (paperListener) {
            socket.ev.off('messages.upsert', paperListener);
            paperListener = null;
        }
        if (paperMasterTimeout) {
            clearTimeout(paperMasterTimeout);
            paperMasterTimeout = null;
        }
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching past papers...' }, { quoted: msg });

        const searchRes = await axios.get(`${API_BASE}/papers`, {
            params: { q: query, limit: 15 },
            headers: { 'x-api-key': API_KEY },
            timeout: 120000
        });

        const searchData = searchRes.data;

        if (!searchData.success || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    `*"${query}" සඳහා කිසිදු Paper එකක් හමු නොවීය!*`,
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const paperList = searchData.data;
        let listText = `📄 *𝗣𝗔𝗦𝗧 𝗣𝗔𝗣𝗘𝗥𝗦 𝗦𝗘𝗔𝗥𝗖𝗛 : _${query}_*\n`;
        listText += `📊 *Total:* ${searchData.total} results\n`;
        listText += `╭──────●➤\n*🔢 ʀᴇ𝗽ʟʏ ʙᴇʟ𝗼ᴡ ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        paperList.forEach((item, index) => {
            listText += `*${index + 1}.* 📘 ${item.title}\n`;
            listText += `    ↳ ${item.grade || 'N/A'} | ${item.subject || 'N/A'} | ${item.year || 'N/A'}\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        paperMasterTimeout = setTimeout(() => {
            clearPaperListeners();
        }, 120000);

        const handlePaperSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation ||
                replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;

            if (!isReply) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= paperList.length) {
                await socket.sendMessage(sender, {
                    text: `❌ කරුණාකර 1 - ${paperList.length} අතර අංකයක් ලබාදෙන්න!`
                }, { quoted: replyMek });
                return;
            }

            clearPaperListeners();
            const selected = paperList[choice];

            await socket.sendMessage(sender, { react: { text: '📥', key: replyMek.key } });

            // File link එක තියෙනවා නම් send කරනවා, නැත්නම් info + external link
            const fileUrl = selected.file_url || selected.external_url;

            let infoText = `📄 *${selected.title}*\n╭──────●➤\n`;
            infoText += `*🎓 Grade:* ${selected.grade || 'N/A'}\n`;
            infoText += `*📚 Subject:* ${selected.subject || 'N/A'}\n`;
            infoText += `*📅 Year:* ${selected.year || 'N/A'}\n`;
            if (selected.exam_board) infoText += `*🏫 Exam Board:* ${selected.exam_board}\n`;
            if (selected.paper_type) infoText += `*📝 Type:* ${selected.paper_type}\n`;
            if (selected.description) infoText += `\n_${selected.description}_\n`;
            infoText += `╰──────────●➤\n`;

            if (fileUrl) {
                infoText += `\n⬇️ _ගොනුව බාගත වෙමින් පවතී..._`;

                await socket.sendMessage(sender, {
                    text: infoText
                }, { quoted: replyMek });

                try {
                    const fileName = `${selected.title.replace(/[^\w\s-]/g, '')}.pdf`;
                    await socket.sendMessage(sender, {
                        document: { url: fileUrl },
                        mimetype: 'application/pdf',
                        fileName: fileName,
                        caption: `✅ *PAPER DOWNLOADED*\n\n📄 ${selected.title}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                    }, { quoted: replyMek });

                    await socket.sendMessage(sender, { react: { text: '✅', key: replyMek.key } });
                } catch (uploadErr) {
                    await socket.sendMessage(sender, {
                        text: `❌ ගොනුව යැවීමේදී දෝෂයක්: ${uploadErr.message}\n\n🔗 *Direct Link:*\n${fileUrl}`
                    }, { quoted: replyMek });
                }
            } else {
                infoText += `\n⚠️ _මෙම Paper එකට direct file link එකක් නොමැත._`;
                await socket.sendMessage(sender, { text: infoText }, { quoted: replyMek });
            }
        };

        paperListener = handlePaperSelection;
        socket.ev.on('messages.upsert', handlePaperSelection);

    } catch (err) {
        clearPaperListeners();
        await socket.sendMessage(sender, {
            text: `❌ Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
            }                    
               
// ==========================================
// DINKAMOVIES - SHAGGY XMD (GDrive + Direct)
// ==========================================
case 'dinka':
case 'dinkamovies':
case 'dinkamovieslk': {
    const DEFAULT_FOOTER = `\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;
    const TEMP_DIR = './tmp_dinka';

    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '🎬 DINKAMOVIES SEARCH',
                '*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න!*\n\n*📌 Usage:* `.dinka ben 10`\n*📌 Usage:* `.dinka the croods`',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const dinkaQuery = args.join(' ').trim();
    const DINKA_API_BASE = 'https://api.chamindu.site/api/v1/movie/dinkamovies';
    const DINKA_API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    let dinkaSelectionListener = null;
    let dinkaOptionListener = null;
    let dinkaMasterTimeout = null;

    const clearAllDinkaListeners = () => {
        if (dinkaSelectionListener) { socket.ev.off('messages.upsert', dinkaSelectionListener); dinkaSelectionListener = null; }
        if (dinkaOptionListener)    { socket.ev.off('messages.upsert', dinkaOptionListener);    dinkaOptionListener    = null; }
        if (dinkaMasterTimeout)     { clearTimeout(dinkaMasterTimeout); dinkaMasterTimeout = null; }
    };

    // ⭐ Parse size
    const parseSizeMB = (s) => {
        if (!s) return 0;
        const m = s.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    // ⭐ Google Drive Download with cookie bypass
    const downloadGdriveFile = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));

        const idMatch = url.match(/(?:id=|\/d\/|file\/d\/)([a-zA-Z0-9_-]+)/);
        if (!idMatch || !idMatch[1]) throw new Error('Invalid Google Drive URL');

        const fileId = idMatch[1];
        console.log(`[GDrive] File ID: ${fileId}`);

        const cookieJar = new Map();
        const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

        const addCookies = (setCookies = []) => {
            setCookies.forEach(c => {
                const [nameVal] = c.split(';');
                const [name, ...valParts] = nameVal.split('=');
                cookieJar.set(name.trim(), valParts.join('=').trim());
            });
        };

        const cookieHeader = () => Array.from(cookieJar.entries()).map(([k, v]) => `${k}=${v}`).join('; ');

        try {
            // Step 1: Get HTML page
            console.log(`[GDrive] Step 1: Fetching HTML...`);

            const firstRes = await axios.get(
                `https://drive.usercontent.google.com/download?id=${fileId}&export=download&authuser=0`,
                {
                    responseType: 'text',
                    timeout: 120000,
                    maxRedirects: 5,
                    headers: {
                        'User-Agent': UA,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9'
                    },
                    validateStatus: () => true
                }
            );

            addCookies(firstRes.headers['set-cookie']);

            const html = firstRes.data || '';
            console.log(`[GDrive] HTML length: ${html.length}`);

            // Step 2: Extract form + inputs
            const actionMatch = html.match(/<form[^>]+action="([^"]+)"/);
            const formAction = actionMatch ? actionMatch[1].replace(/&amp;/g, '&') : null;

            const inputs = {};
            let m;

            const regex1 = /<input[^>]+name="([^"]+)"[^>]+value="([^"]*)"/g;
            while ((m = regex1.exec(html)) !== null) inputs[m[1]] = m[2];

            const regex2 = /<input[^>]+value="([^"]*)"[^>]+name="([^"]+)"/g;
            while ((m = regex2.exec(html)) !== null) inputs[m[2]] = m[1];

            console.log(`[GDrive] Form: ${formAction ? 'found' : 'none'}, Inputs: ${JSON.stringify(Object.keys(inputs))}`);

            // Step 3: Build download URL
            let downloadUrl = '';
            if (formAction) {
                const params = new URLSearchParams();
                for (const [k, v] of Object.entries(inputs)) params.append(k, v);
                downloadUrl = `${formAction}?${params.toString()}`;
            } else {
                const params = new URLSearchParams({
                    id: fileId,
                    export: 'download',
                    confirm: 't'
                });
                if (inputs.uuid) params.append('uuid', inputs.uuid);
                if (inputs.at) params.append('at', inputs.at);
                downloadUrl = `https://drive.usercontent.google.com/download?${params.toString()}`;
            }

            console.log(`[GDrive] Step 2: URL: ${downloadUrl.substring(0, 100)}...`);

            // Step 4: Download
            const writer = fs.createWriteStream(dest);
            const fileRes = await axios({
                url: downloadUrl,
                method: 'GET',
                responseType: 'stream',
                timeout: 0,
                maxRedirects: 10,
                maxContentLength: Infinity,
                maxBodyLength: Infinity,
                headers: {
                    'User-Agent': UA,
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cookie': cookieHeader(),
                    'Referer': `https://drive.usercontent.google.com/download?id=${fileId}&export=download`,
                    'Origin': 'https://drive.usercontent.google.com'
                }
            });

            const ct = (fileRes.headers['content-type'] || '').toLowerCase();
            console.log(`[GDrive] Content-Type: ${ct}`);

            if (ct.includes('text/html')) {
                fileRes.data.destroy();
                throw new Error('Google Drive returned HTML');
            }

            fileRes.data.pipe(writer);
            await new Promise((resolve, reject) => {
                writer.on('finish', resolve);
                writer.on('error', reject);
                fileRes.data.on('error', reject);
            });

            const stats = await fs.stat(dest);
            const sizeMB = stats.size / 1024 / 1024;
            console.log(`[GDrive] ✅ Downloaded: ${sizeMB.toFixed(1)} MB`);

            if (sizeMB < 1) {
                await fs.remove(dest).catch(() => {});
                throw new Error(`File too small: ${sizeMB.toFixed(2)} MB`);
            }

            return { success: true, size: stats.size };

        } catch (err) {
            console.error('[GDrive] ❌ Error:', err.message);
            throw err;
        }
    };

    // ⭐ Direct download (non-GDrive)
    const downloadDirect = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 10,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*'
            }
        });

        const ct = (res.headers['content-type'] || '').toLowerCase();
        if (ct.includes('text/html')) {
            res.data.destroy();
            throw new Error('HTML response (not a file)');
        }

        res.data.pipe(writer);
        await new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });

        const stats = await fs.stat(dest);
        if (stats.size < 1024 * 100) {
            await fs.remove(dest).catch(() => {});
            throw new Error(`File too small: ${(stats.size / 1024).toFixed(1)} KB`);
        }

        return { success: true, size: stats.size };
    };

    // ⭐ Smart downloader
    const downloadSmart = async (url, dest) => {
        if (url.includes('drive.google.com') ||
            url.includes('drive.usercontent.google.com') ||
            url.includes('docs.google.com')) {
            return await downloadGdriveFile(url, dest);
        }
        return await downloadDirect(url, dest);
    };

    try {
        await socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗦𝗘𝗔𝗥𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🔍 *Searching DinkaMovies for:* _${dinkaQuery}_\n⚡ _Please wait..._`
        }, { quoted: msg });

        // ═══ STEP 1 : SEARCH ═══
        const searchRes = await axios.get(`${DINKA_API_BASE}/search`, {
            params: { q: dinkaQuery, api_key: DINKA_API_KEY },
            timeout: 120000
        });

        const searchData = searchRes.data;
        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    `*"${dinkaQuery}"* සඳහා කිසිදු ප්‍රතිඵලයක් හමු නොවීය!`,
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const dinkaList = searchData.data.slice(0, 20);
        let listText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗜𝗡𝗞𝗔𝗠𝗢𝗩𝗜𝗘𝗦 ❫*\n\n🎯 *Query:* _${dinkaQuery}_\n📊 *Results:* _${dinkaList.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;

        dinkaList.forEach((item, index) => {
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            listText += `*${num}* ➜ 🍿 _${item.title}*\n    ↳ (📅 ${item.year || 'N/A'})\n`;
        });
        listText += `\n📌 _Reply with number to download!_${DEFAULT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: dinkaList[0].poster || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;
        dinkaMasterTimeout = setTimeout(clearAllDinkaListeners, 180000);

        // ═══ STEP 2 : USER PICKS MOVIE ═══
        const handleDinkaSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;
            if (!isReply) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= dinkaList.length) {
                return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${dinkaList.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: replyMek });
            }

            if (dinkaSelectionListener) { socket.ev.off('messages.upsert', dinkaSelectionListener); dinkaSelectionListener = null; }

            const chosenItem = dinkaList[choice];
            await socket.sendMessage(sender, {
                text: `⏳ *"${chosenItem.title}"* තොරතුරු සහ Download options ලබා ගනිමින්...`
            }, { quoted: replyMek });

            try {
                // ═══ STEP 3 : INFO ═══
                const infoRes = await axios.get(`${DINKA_API_BASE}/infodl`, {
                    params: { q: chosenItem.url, api_key: DINKA_API_KEY },
                    timeout: 120000
                });

                const mediaData = infoRes.data?.data;
                const downloads = mediaData?.downloads || [];

                if (!mediaData || downloads.length === 0) {
                    throw new Error('බාගත කිරීමේ links හමු නොවීය.');
                }

                const isTv = mediaData.type === 'tv_series' || downloads[0].episode !== undefined;

                let infoText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗜𝗡𝗞𝗔𝗠𝗢𝗩𝗜𝗘𝗦 ❫*\n\n`;
                infoText += `🎬 *${mediaData.title}*\n`;
                if (mediaData.genres?.length) infoText += `🎭 *Genres:* ${mediaData.genres.join(', ')}\n`;
                infoText += `\n`;

                if (isTv) {
                    infoText += `📺 *Type:* TV Series / Animation\n`;
                    infoText += `🔢 *Total Episodes:* ${downloads.length}\n\n`;
                    infoText += `*Available Episodes:*\n`;
                    downloads.forEach((dl, i) => {
                        infoText += `*${i + 1}.* ${dl.title || dl.name || `Episode ${i + 1}`}\n`;
                    });
                } else {
                    infoText += `🎥 *Type:* Movie\n\n`;
                    infoText += `*Available Qualities:*\n`;
                    downloads.forEach((dl, i) => {
                        const typeBadge = dl.type ? `[${dl.type}]` : '';
                        const sizeMB = parseSizeMB(dl.size);
                        let note = '';
                        if (sizeMB > 2000) note = ' ⚠️';
                        else if (sizeMB > 0) note = ' ✓';
                        infoText += `*${i + 1}.* ${dl.quality || 'Download'} ${dl.size ? `┃ 📦 ${dl.size}` : ''} ${typeBadge}${note}\n`;
                    });
                }
                infoText += `\n👉 *බාගත කිරීමට අදාළ අංකය Reply කරන්න.*\n_✓ = Document • ⚠️ = 2GB+_`;

                const infoMsg = await socket.sendMessage(sender, {
                    image: { url: mediaData.poster || chosenItem.poster || pickBotImage(sessionConfig) },
                    caption: infoText
                }, { quoted: replyMek });

                const infoMsgID = infoMsg.key.id;

                // ═══ STEP 4 : USER PICKS DOWNLOAD ═══
                const handleOptionSelection = async ({ messages: optMessages }) => {
                    const optMek = optMessages?.[0];
                    if (!optMek?.message || optMek.key.remoteJid !== sender) return;

                    const optText = (optMek.message.conversation || optMek.message.extendedTextMessage?.text || '').trim();
                    const isOptReply = optMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;
                    if (!isOptReply) return;

                    const optIdx = parseInt(optText) - 1;
                    if (isNaN(optIdx) || optIdx < 0 || optIdx >= downloads.length) {
                        return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${downloads.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: optMek });
                    }

                    clearAllDinkaListeners();

                    const selectedOption = downloads[optIdx];
                    const rawUrl = selectedOption.direct_link || selectedOption.download_link || selectedOption.link || '';
                    const sizeMB = parseSizeMB(selectedOption.size);

                    const cleanTitle = (mediaData.title || chosenItem.title).replace(/[^a-zA-Z0-9 ]/g, '').trim().substring(0, 50);
                    const optLabel = (selectedOption.title || selectedOption.quality || `Part_${optIdx + 1}`).replace(/[^a-zA-Z0-9 ]/g, '').trim();
                    const fileName = `${cleanTitle} - ${optLabel}.mp4`;

                    await socket.sendMessage(sender, { react: { text: '📥', key: optMek.key } });

                    // ⚠️ 2GB limit
                    if (sizeMB > 2000) {
                        return socket.sendMessage(sender, {
                            text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${mediaData.title}*\n📌 *${selectedOption.quality}*\n📦 *${selectedOption.size}*\n\n🔗 *Direct Link:*\n${rawUrl}\n\n_IDM එකෙන් download කරන්න._${DEFAULT_FOOTER}`
                        }, { quoted: optMek });
                    }

                    await socket.sendMessage(sender, {
                        text: `⏳ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚*\n\n📌 *${selectedOption.title || selectedOption.quality}*\n📦 *Size:* ${selectedOption.size || 'N/A'}\n\n_කරුණාකර රැඳී සිටින්න..._`
                    }, { quoted: optMek });

                    // ⭐ Server download
                    await fs.ensureDir(TEMP_DIR);
                    const safeName = cleanTitle.replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 50);
                    const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.mp4`);

                    try {
                        await downloadSmart(rawUrl, localFile);

                        const stats = await fs.stat(localFile);
                        const realSizeMB = stats.size / 1024 / 1024;

                        // ⚠️ Error page check
                        if (realSizeMB < 1) {
                            await fs.remove(localFile).catch(() => {});
                            throw new Error('File too small (error page)');
                        }

                        await socket.sendMessage(sender, {
                            text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                        }, { quoted: optMek });

                        // ⭐ Send as document
                        try {
                            await socket.sendMessage(sender, {
                                document: { url: localFile },
                                mimetype: 'video/mp4',
                                fileName: fileName,
                                caption: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗜𝗡𝗞𝗔𝗠𝗢𝗩𝗜𝗘𝗦*\n\n🎬 *Title:* ${mediaData.title}\n📌 *Option:* ${selectedOption.title || selectedOption.quality || 'Direct'}\n📦 *Size:* ${realSizeMB.toFixed(1)} MB\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
                            }, { quoted: optMek });

                            await socket.sendMessage(sender, { react: { text: '✅', key: optMek.key } });

                        } catch (sendErr) {
                            await socket.sendMessage(sender, {
                                text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${rawUrl}\n\n_IDM එකෙන් download කරන්න._`
                            }, { quoted: optMek });
                        }

                        // Cleanup
                        await fs.remove(localFile).catch(() => {});

                    } catch (downloadErr) {
                        console.error('[Dinka] download error:', downloadErr.message);

                        await socket.sendMessage(sender, {
                            text: `⚠️ *Direct Download*\n\n🎬 *${mediaData.title}*\n📌 *${selectedOption.quality}*\n📦 *${selectedOption.size || 'N/A'}*\n\n🔗 *Download Link:*\n${rawUrl}\n\n💡 _IDM එකෙන් download කරන්න._\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
                        }, { quoted: optMek });

                        try { await fs.remove(localFile); } catch {}
                    }
                };

                dinkaOptionListener = handleOptionSelection;
                socket.ev.on('messages.upsert', dinkaOptionListener);

            } catch (infoErr) {
                clearAllDinkaListeners();
                console.error('[Dinka] info error:', infoErr.message);
                await socket.sendMessage(sender, {
                    text: `❌ DinkaMovies Info Error: ${infoErr.message}`
                }, { quoted: replyMek });
            }
        };

        dinkaSelectionListener = handleDinkaSelection;
        socket.ev.on('messages.upsert', dinkaSelectionListener);

    } catch (err) {
        clearAllDinkaListeners();
        console.error('[Dinka] error:', err.message);
        await socket.sendMessage(sender, {
            text: `❌ DinkaMovies Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
}
case 'moviehubbd':
case 'mhbd':
case 'bw': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර Movie නම ලබාදෙන්න! උදා: .mhbd DC*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const query = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/movies/moviehubbd';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';
    const MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024; // 2GB
    const REQUEST_TIMEOUT = 600000; // 10 min

    // ============================================
    // 🔧 IMPORTS (case එක ඇතුලේ require කරනවා)
    // ============================================
    const os = (await import('os')).default; // ESM-safe (fs/path come from the top-level imports)

    // ============================================
    // 🔧 HELPERS
    // ============================================

    // Size string -> bytes ("1.97 GB" -> 2115271393)
    const parseSizeToBytes = (sizeStr) => {
        if (!sizeStr || sizeStr === 'N/A') return null;
        const match = sizeStr.match(/([\d.]+)\s*(GB|MB|KB|B)/i);
        if (!match) return null;
        const val = parseFloat(match[1]);
        const unit = match[2].toUpperCase();
        const mult = { 'B': 1, 'KB': 1024, 'MB': 1024 ** 2, 'GB': 1024 ** 3 };
        return Math.round(val * (mult[unit] || 0));
    };

    // Bytes -> Human readable
    const formatBytes = (bytes) => {
        if (!bytes || bytes === 0) return 'N/A';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
    };

    // URL fix
    const fixDownloadUrl = (url) => {
        if (!url || url === '#') return url;
        try {
            let fixed = url.replace(/^Https/i, 'https');
            const urlObj = new URL(fixed);
            urlObj.pathname = decodeURIComponent(urlObj.pathname);
            return urlObj.toString();
        } catch (e) { return url; }
    };

    // HTML page එකෙන් real MP4 URL එක extract කරනවා
    const extractRealMp4Url = async (pageUrl) => {
        try {
            const res = await axios.get(pageUrl, {
                timeout: 120000,
                maxRedirects: 10,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Referer': 'https://moviehubbd.net/'
                }
            });

            const html = typeof res.data === 'string' ? res.data : JSON.stringify(res.data);

            // Pattern 1: <video src="...mp4">
            let m = html.match(/<video[^>]+src=["']([^"']+\.mp4[^"']*)["']/i);
            if (m) return m[1].replace(/&amp;/g, '&');

            // Pattern 2: <source src="...mp4">
            m = html.match(/<source[^>]+src=["']([^"']+\.mp4[^"']*)["']/i);
            if (m) return m[1].replace(/&amp;/g, '&');

            // Pattern 3: <a href="...mp4">
            m = html.match(/<a[^>]+href=["']([^"']+\.mp4[^"']*)["']/i);
            if (m) return m[1].replace(/&amp;/g, '&');

            // Pattern 4: JSON/JS ඇතුලේ .mp4 URL
            m = html.match(/["'](https?:\/\/[^"'\s]+\.mp4[^"'\s]*)["']/i);
            if (m) return m[1].replace(/&amp;/g, '&');

            // Pattern 5: onclick location.href
            m = html.match(/onclick=["'][^"']*(?:href|location)\s*=\s*["']([^"']+)["']/i);
            if (m) return m[1].replace(/&amp;/g, '&');

            return null;
        } catch (e) {
            console.log('[MHBD Extract] Error:', e.message);
            return null;
        }
    };

    // File එක temp folder එකට download කරනවා (streaming)
    const downloadFileToTemp = async (url, referer = 'https://moviehubbd.net/') => {
        const tmpPath = path.join(
            os.tmpdir(),
            `mhbd_${Date.now()}_${Math.random().toString(36).substring(7)}.mp4`
        );

        const writer = fs.createWriteStream(tmpPath);
        const response = await axios({
            method: 'get',
            url: url,
            responseType: 'stream',
            timeout: REQUEST_TIMEOUT,
            maxRedirects: 10,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': referer,
                'Accept': '*/*',
                'Range': 'bytes=0-'
            }
        });

        const ct = response.headers['content-type'] || '';
        if (ct.includes('text/html')) {
            writer.close();
            try { fs.unlinkSync(tmpPath); } catch (e) {}
            throw new Error('HTML page එකක් ලැබුණා - real MP4 නෙවෙයි');
        }

        return new Promise((resolve, reject) => {
            response.data.pipe(writer);
            writer.on('finish', () => {
                const stats = fs.statSync(tmpPath);
                resolve({ path: tmpPath, size: stats.size });
            });
            writer.on('error', (err) => {
                try { fs.unlinkSync(tmpPath); } catch (e) {}
                reject(err);
            });
            response.data.on('error', (err) => {
                try { fs.unlinkSync(tmpPath); } catch (e) {}
                reject(err);
            });
        });
    };

    // Session management
    const sessionKey = `mhbd_${sender}`;
    if (global.mhbdSessions?.[sessionKey]) {
        const old = global.mhbdSessions[sessionKey];
        if (old.listener) socket.ev.off('messages.upsert', old.listener);
        if (old.timeout) clearTimeout(old.timeout);
    }
    if (!global.mhbdSessions) global.mhbdSessions = {};

    const cleanup = () => {
        const s = global.mhbdSessions[sessionKey];
        if (s?.listener) socket.ev.off('messages.upsert', s.listener);
        if (s?.timeout) clearTimeout(s.timeout);
        delete global.mhbdSessions[sessionKey];
    };

    // ============================================
    // 🎬 MAIN LOGIC
    // ============================================

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching on MovieHubBD...' }, { quoted: msg });

        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: query, api_key: API_KEY },
            timeout: 120000
        });

        const searchData = searchRes.data;
        if (!searchData.status || !searchData.data?.length) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*කිසිදු Movie එකක් හමු නොවීය!*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const results = searchData.data.slice(0, 15);
        let listText = `🎬 *𝗠𝗢𝗩𝗜𝗘𝗛𝗨𝗕𝗕𝗗 𝗦𝗘𝗔𝗥𝗖𝗛 : _${query}_*\n╭──────●➤\n*🔢 ʀᴇ𝗽ʟʏ ʙᴇʟ𝗼ᴡ ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        results.forEach((item, index) => {
            const shortTitle = item.title
                .replace(/\s*\(\d{4}\).*$/, '')
                .replace(/\s*Dual Audio.*$/i, '')
                .replace(/\s*\[.*$/, '')
                .trim();
            const year = item.year ? ` (${item.year})` : '';
            const quality = item.quality ? ` | ${item.quality}` : '';
            const rating = item.rating && item.rating !== 'N/A' ? ` | ⭐ ${item.rating}` : '';
            listText += `*🎥 ${index + 1} ┃❭❭ ${shortTitle}${year}*\n    ↳${quality}${rating}\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: results[0].image || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        // ============================================
        // 📌 HANDLER 1: Movie Selection
        // ============================================
        const handleSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;
            if (replyMek.key.fromMe && !/^\d{1,3}$/.test(extractIncoming(replyMek).text)) return; // own-number test: allow typed/tapped numbers

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;
            if (!isReply) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= results.length) {
                await socket.sendMessage(sender, {
                    text: `❌ කරුණාකර 1 - ${results.length} අතර අංකයක් ලබාදෙන්න!`
                }, { quoted: replyMek });
                return;
            }

            socket.ev.off('messages.upsert', handleSelection);
            const chosen = results[choice];

            await socket.sendMessage(sender, { text: '⏳ Fetching details & download links...' }, { quoted: replyMek });

            try {
                const infoRes = await axios.get(`${API_BASE}/infodl`, {
                    params: { q: chosen.link, api_key: API_KEY },
                    timeout: 120000
                });

                const data = infoRes.data?.data;
                if (!data) throw new Error('Details හමු නොවීය.');

                const validDownloads = (data.downloads || [])
                    .filter(d => d.link && d.link !== '#' && d.link.length > 10)
                    .map(d => ({
                        ...d,
                        link: fixDownloadUrl(d.link),
                        fallback: d.fallback_url ? fixDownloadUrl(d.fallback_url) : null,
                        sizeBytes: parseSizeToBytes(d.size)
                    }))
                    .filter(d => d.link.startsWith('http'));

                if (validDownloads.length === 0) throw new Error('බාගත කළ හැකි links හමු නොවීය.');

                let infoText = `🎬 *${data.title?.substring(0, 100) || chosen.title}*\n\n`;
                infoText += `⭐ *IMDb:* ${data.imdb || 'N/A'}\n`;
                infoText += `📅 *Year:* ${data.year || 'N/A'}\n`;
                infoText += `⏱️ *Runtime:* ${data.runtime || 'N/A'}\n`;
                infoText += `🎭 *Genres:* ${data.genres?.join(', ') || 'N/A'}\n`;
                if (data.language) infoText += `🗣️ *Language:* ${data.language}\n`;
                if (data.director) infoText += `🎬 *Director:* ${data.director}\n`;
                if (data.cast?.length) infoText += `👥 *Cast:* ${data.cast.slice(0, 3).join(', ')}\n`;
                infoText += `\n*📥 Available Downloads (${validDownloads.length}):*\n`;

                validDownloads.forEach((dl, i) => {
                    infoText += `*${i + 1}.* ${dl.quality || 'N/A'}`;
                    if (dl.size && dl.size !== 'N/A') infoText += ` — ${dl.size}`;
                    infoText += `\n`;
                });

                infoText += `\n👉 *බාගත කිරීමට අදාළ අංකය Reply කරන්න.*`;
                infoText += `\n_⚠️ 2GB ට අඩු files පමණක් auto-send වේ_`;

                if (data.story && data.story.length > 100) {
                    await socket.sendMessage(sender, {
                        text: `📖 *Story:*\n\n${data.story.substring(0, 800)}${data.story.length > 800 ? '...' : ''}`
                    }, { quoted: replyMek });
                }

                const infoMsg = await socket.sendMessage(sender, {
                    image: { url: data.image || chosen.image },
                    caption: infoText
                }, { quoted: replyMek });

                const infoMsgID = infoMsg.key.id;

                // ============================================
                // 📌 HANDLER 2: Download Selection
                // ============================================
                const handleDownload = async ({ messages: dlMsgs }) => {
                    const dlMek = dlMsgs?.[0];
                    if (!dlMek?.message || dlMek.key.remoteJid !== sender) return;
                    if (dlMek.key.fromMe && !/^\d{1,3}$/.test(extractIncoming(dlMek).text)) return;

                    const dlText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                    const isDlReply = dlMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;
                    if (!isDlReply) return;

                    const dlIdx = parseInt(dlText) - 1;
                    if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= validDownloads.length) {
                        await socket.sendMessage(sender, {
                            text: `❌ කරුණාකර 1 - ${validDownloads.length} අතර අංකයක් ලබාදෙන්න!`
                        }, { quoted: dlMek });
                        return;
                    }

                    cleanup();
                    const selected = validDownloads[dlIdx];

                    // Clean filename
                    const cleanTitle = (data.original_title || data.title || chosen.title)
                        .replace(/\(.*?\)/g, '').replace(/\[.*?\]/g, '')
                        .replace(/Dual Audio.*$/i, '').replace(/WEB-DL.*$/i, '')
                        .replace(/[^\w\s-]/g, '').replace(/\s+/g, ' ').trim().substring(0, 50);
                    const year = data.year || '';
                    const quality = selected.quality || 'HD';
                    const fileName = `${cleanTitle} ${year} ${quality}.mp4`.replace(/\s+/g, ' ');

                    await socket.sendMessage(sender, { react: { text: '📥', key: dlMek.key } });

                    // 2GB Check
                    const fileSize = selected.sizeBytes;
                    if (fileSize && fileSize > MAX_FILE_SIZE) {
                        await socket.sendMessage(sender, {
                            text: `⚠️ *FILE TOO LARGE*\n\n` +
                                  `📁 *File:* ${fileName}\n` +
                                  `📊 *Size:* ${formatBytes(fileSize)}\n` +
                                  `❌ *Limit:* 2 GB\n\n` +
                                  `_පහත direct link එකෙන් බාගත කරගන්න 👇_\n\n` +
                                  `🔗 ${selected.link}`
                        }, { quoted: dlMek });
                        await socket.sendMessage(sender, { react: { text: '⚠️', key: dlMek.key } });
                        return;
                    }

                    const sizeMsg = fileSize 
                        ? `✅ *Size:* ${formatBytes(fileSize)}\n` 
                        : `📊 *Size:* ${selected.size || 'N/A'}\n`;

                    await socket.sendMessage(sender, {
                        text: `${sizeMsg}⏳ *Step 1/2:* Extracting real link...`
                    }, { quoted: dlMek });

                    let tmpPath = null;

                    try {
                        // STEP 1: Real MP4 URL extract
                        let realUrl = await extractRealMp4Url(selected.link);

                        if (!realUrl && selected.fallback) {
                            realUrl = await extractRealMp4Url(selected.fallback);
                        }

                        // Real URL නොහමුවොත් original link එක try කරන්න
                        if (!realUrl) {
                            realUrl = selected.link;
                            console.log('[MHBD] Real URL හමු නොවීය - original link try');
                        }

                        await socket.sendMessage(sender, {
                            text: `📥 *Step 2/2:* Downloading...\n⏱️ _විනාඩි 2-10ක් ගත විය හැක_\n\n_කරුණාකර රැඳී සිටින්න..._`
                        }, { quoted: dlMek });

                        // STEP 2: File download
                        const result = await downloadFileToTemp(realUrl, 'https://moviehubbd.net/');
                        tmpPath = result.path;

                        // Downloaded size check
                        if (result.size > MAX_FILE_SIZE) {
                            try { fs.unlinkSync(tmpPath); } catch (e) {}
                            await socket.sendMessage(sender, {
                                text: `⚠️ *FILE TOO LARGE*\n\n📊 *Downloaded:* ${formatBytes(result.size)}\n❌ *Limit:* 2 GB\n\n🔗 ${selected.link}`
                            }, { quoted: dlMek });
                            return;
                        }

                        // STEP 3: Document MP4 විදිහට යවන්න
                        await socket.sendMessage(sender, {
                            document: fs.readFileSync(tmpPath),
                            mimetype: 'video/mp4',
                            fileName: fileName,
                            caption: `✅ *DOWNLOAD COMPLETE*\n\n` +
                                     `🎬 *Title:* ${cleanTitle}\n` +
                                     `📅 *Year:* ${year || 'N/A'}\n` +
                                     `🎞️ *Quality:* ${quality}\n` +
                                     `🗣️ *Language:* ${data.language || 'Hindi'}\n` +
                                     `📊 *Size:* ${formatBytes(result.size)}\n` +
                                     `> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: dlMek });

                        await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });

                    } catch (err) {
                        console.log('[MHBD Download] Error:', err.message);

                        await socket.sendMessage(sender, {
                            text: `❌ *DOWNLOAD FAILED*\n\n` +
                                  `_${err.message}_\n\n` +
                                  `🔗 *Direct Link (ඔයාටම try කරන්න):*\n${selected.link}\n\n` +
                                  (selected.fallback ? `*Fallback:*\n${selected.fallback}` : '')
                        }, { quoted: dlMek });
                        await socket.sendMessage(sender, { react: { text: '❌', key: dlMek.key } });

                    } finally {
                        // STEP 4: Temp file delete
                        if (tmpPath && fs.existsSync(tmpPath)) {
                            try { fs.unlinkSync(tmpPath); } catch (e) {}
                        }
                    }
                };

                global.mhbdSessions[sessionKey].listener = handleDownload;
                socket.ev.on('messages.upsert', handleDownload);

            } catch (infoErr) {
                cleanup();
                await socket.sendMessage(sender, { text: `❌ Info Error: ${infoErr.message}` }, { quoted: replyMek });
            }
        };

        global.mhbdSessions[sessionKey] = {
            listener: handleSelection,
            timeout: setTimeout(() => cleanup(), 300000) // 5 min
        };

        socket.ev.on('messages.upsert', handleSelection);

    } catch (err) {
        cleanup();
        await socket.sendMessage(sender, { text: `❌ Error: ${err.message}` }, { quoted: msg });
    }
    break;
}
case 'movie':
case 'mv':
case 'cineverse':
case 'cv': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර Movie/Series නම ලබාදෙන්න! උදා: .movie Vikings*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const query = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/movies/cineverselk';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    // ⚙️ CONFIG
    const MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024; // 2GB
    const SIZE_CHECK_TIMEOUT = 10000; // 10s

    // ============================================
    // 🔧 HELPER FUNCTIONS (case එක ඇතුලේම)
    // ============================================

    // URL fix (Https -> https + decode)
    const fixDownloadUrl = (url) => {
        if (!url || url === '#') return url;
        try {
            let fixed = url.replace(/^Https/i, 'https');
            const urlObj = new URL(fixed);
            urlObj.pathname = decodeURIComponent(urlObj.pathname);
            return urlObj.toString();
        } catch (e) {
            return url.replace(/^Https/i, 'https')
                .replace(/%28/g, '(').replace(/%29/g, ')')
                .replace(/%5B/g, '[').replace(/%5D/g, ']');
        }
    };

    // File size එක HEAD request එකෙන් ගන්නවා
    const getRemoteFileSize = async (url) => {
        try {
            const res = await axios.head(url, {
                timeout: SIZE_CHECK_TIMEOUT,
                maxRedirects: 5,
                validateStatus: () => true
            });
            const len = res.headers['content-length'];
            return len ? parseInt(len) : null;
        } catch (e) {
            return null;
        }
    };

    // Bytes -> Human readable
    const formatBytes = (bytes) => {
        if (!bytes || bytes === 0) return 'N/A';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
    };

    // Session key
    const sessionKey = `cineverse_${sender}`;

    // පරණ session එකක් තියෙනවනම් clear
    if (global.cineverseSessions?.[sessionKey]) {
        const old = global.cineverseSessions[sessionKey];
        if (old.listener) socket.ev.off('messages.upsert', old.listener);
        if (old.timeout) clearTimeout(old.timeout);
    }
    if (!global.cineverseSessions) global.cineverseSessions = {};

    // Cleanup function
    const cleanup = () => {
        const s = global.cineverseSessions[sessionKey];
        if (s?.listener) socket.ev.off('messages.upsert', s.listener);
        if (s?.timeout) clearTimeout(s.timeout);
        delete global.cineverseSessions[sessionKey];
    };

    // ============================================
    // 🎬 MAIN LOGIC
    // ============================================

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching on CineVerseLK...' }, { quoted: msg });

        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: query, api_key: API_KEY },
            timeout: 120000
        });

        const searchData = searchRes.data;
        if (!searchData.status || !searchData.data?.length) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*කිසිදු Movie/Series එකක් හමු නොවීය!*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const results = searchData.data.slice(0, 15);
        let listText = `🎬 *𝗖𝗜𝗡𝗘𝗩𝗘𝗥𝗦𝗘 𝗟𝗞 𝗦𝗘𝗔𝗥𝗖𝗛 : _${query}_*\n╭──────●➤\n*🔢 ʀᴇ𝗽ʟʏ ʙᴇʟ𝗼ᴡ ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        results.forEach((item, index) => {
            const icon = item.type === 'series' ? '📺' : '🎥';
            listText += `*${icon} ${index + 1} ┃❭❭ ${item.title} (${item.year || 'N/A'})*\n    ↳ ⭐ ${item.imdb || 'N/A'} | ${item.type?.toUpperCase() || 'MOVIE'}\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: results[0].image || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        const handleSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message) return;
            if (replyMek.key.fromMe && !/^\d{1,3}$/.test(extractIncoming(replyMek).text)) return; // own-number test: allow typed/tapped numbers
            if (!sameChat(replyMek.key, sender)) return;

            // quoted reply OR bare number, ephemeral/wrapped messages, button taps - all handled
            const { text } = extractIncoming(replyMek);
            if (!isReplyTo(replyMek, searchMsgID)) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= results.length) {
                await socket.sendMessage(sender, {
                    text: `❌ කරුණාකර 1 - ${results.length} අතර අංකයක් ලබාදෙන්න!`
                }, { quoted: replyMek });
                return;
            }

            socket.ev.off('messages.upsert', handleSelection);
            const chosen = results[choice];

            await socket.sendMessage(sender, { text: '⏳ Fetching details & download links...' }, { quoted: replyMek });

            try {
                const infoRes = await axios.get(`${API_BASE}/infodl`, {
                    params: { q: chosen.link, api_key: API_KEY },
                    timeout: 120000
                });

                const data = infoRes.data?.data;
                if (!data) throw new Error('Details හමු නොවීය.');

                // Valid downloads filter + URL fix
                const validDownloads = (data.downloads || [])
                    .filter(d => d.link && d.link !== '#' && d.link.length > 10)
                    .map(d => ({ ...d, link: fixDownloadUrl(d.link) }))
                    .filter(d => d.link.startsWith('http'));

                if (validDownloads.length === 0) {
                    throw new Error('බාගත කළ හැකි links හමු නොවීය.');
                }

                let infoText = `🎬 *${data.title} (${data.year || 'N/A'})*\n\n`;
                infoText += `⭐ *IMDb:* ${data.imdb || 'N/A'}\n`;
                infoText += `🎭 *Genres:* ${data.tags?.join(', ') || 'N/A'}\n`;
                if (data.cast) infoText += `👥 *Cast:* ${data.cast}\n`;
                infoText += `\n`;

                if (data.is_series && data.episodes_per_season) {
                    infoText += `📺 *Seasons:* ${Object.keys(data.episodes_per_season).join(', ')}\n\n`;
                }

                infoText += `*📥 Available Downloads (${validDownloads.length}):*\n`;
                validDownloads.slice(0, 30).forEach((dl, i) => {
                    const tag = dl.season ? `S${dl.season}E${dl.episode}` : '';
                    infoText += `*${i + 1}.* ${tag} ${dl.quality || ''}\n`;
                });

                if (validDownloads.length > 30) {
                    infoText += `\n_...තවත් ${validDownloads.length - 30} ක් ඇත_`;
                }
                infoText += `\n👉 *බාගත කිරීමට අදාළ අංකය Reply කරන්න.*`;
                infoText += `\n_⚠️ 2GB ට අඩු files පමණක් auto-send වේ_`;

                if (data.story && data.story.length > 100) {
                    await socket.sendMessage(sender, {
                        text: `📖 *Story:*\n\n${data.story.substring(0, 800)}${data.story.length > 800 ? '...' : ''}`
                    }, { quoted: replyMek });
                }

                const infoMsg = await socket.sendMessage(sender, {
                    image: { url: data.image || chosen.image },
                    caption: infoText
                }, { quoted: replyMek });

                const infoMsgID = infoMsg.key.id;

                const handleDownload = async ({ messages: dlMsgs }) => {
                    const dlMek = dlMsgs?.[0];
                    if (!dlMek?.message) return;
                    if (dlMek.key.fromMe && !/^\d{1,3}$/.test(extractIncoming(dlMek).text)) return;
                    if (!sameChat(dlMek.key, sender)) return;

                    const { text: dlText } = extractIncoming(dlMek);
                    if (!isReplyTo(dlMek, infoMsgID)) return;

                    const dlIdx = parseInt(dlText) - 1;
                    if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= validDownloads.length) {
                        await socket.sendMessage(sender, {
                            text: `❌ කරුණාකර 1 - ${validDownloads.length} අතර අංකයක් ලබාදෙන්න!`
                        }, { quoted: dlMek });
                        return;
                    }

                    cleanup();
                    const selected = validDownloads[dlIdx];
                    const tag = selected.season
                        ? `S${String(selected.season).padStart(2, '0')}E${String(selected.episode).padStart(2, '0')}`
                        : 'Movie';
                    const cleanTitle = data.title.replace(/[^\w\s-]/g, '').trim();
                    const fileName = `${cleanTitle} ${tag} ${selected.quality || 'HD'}.mp4`.replace(/\s+/g, ' ');

                    await socket.sendMessage(sender, { react: { text: '📥', key: dlMek.key } });
                    await socket.sendMessage(sender, {
                        text: `🔍 *Checking file size...*\n_${fileName}_`
                    }, { quoted: dlMek });

                    const fileSize = await getRemoteFileSize(selected.link);

                    // Size check fail - try කරන්නම දෙන්න
                    if (fileSize === null) {
                        await socket.sendMessage(sender, {
                            text: `⚠️ File size එක check කරන්න බැරි වුණා. Try කරමු...\n_${fileName}_`
                        }, { quoted: dlMek });
                    }
                    // 2GB ට වැඩි - direct link යවන්න
                    else if (fileSize > MAX_FILE_SIZE) {
                        await socket.sendMessage(sender, {
                            text: `⚠️ *FILE TOO LARGE*\n\n` +
                                  `📁 *File:* ${fileName}\n` +
                                  `📊 *Size:* ${formatBytes(fileSize)}\n` +
                                  `❌ *Limit:* 2 GB\n\n` +
                                  `_මේ file එක WhatsApp හරහා auto-send කරන්න බෑ._\n` +
                                  `_පහත direct link එකෙන් බාගත කරගන්න 👇_\n\n` +
                                  `🔗 ${selected.link}`
                        }, { quoted: dlMek });
                        await socket.sendMessage(sender, { react: { text: '⚠️', key: dlMek.key } });
                        return;
                    }
                    // Size OK
                    else {
                        await socket.sendMessage(sender, {
                            text: `✅ *Size OK:* ${formatBytes(fileSize)}\n` +
                                  `📥 *Downloading & Uploading...*\n` +
                                  `⏱️ _මෙයට විනාඩි 2ක් පමණ ගත විය හැක_\n\n` +
                                  `_කරුණාකර රැඳී සිටින්න..._`
                        }, { quoted: dlMek });
                    }

                    try {
                        // Document MP4 විදිහට යවනවා
                        await socket.sendMessage(sender, {
                            document: { url: selected.link },
                            mimetype: 'video/mp4',
                            fileName: fileName,
                            caption: `✅ *DOWNLOAD COMPLETE*\n\n` +
                                     `🎬 *Title:* ${data.title}\n` +
                                     `📌 *Episode:* ${tag}\n` +
                                     `🎞️ *Quality:* ${selected.quality || 'N/A'}\n` +
                                     `🗣️ *Language:* ${selected.language || 'Sinhala Sub'}\n` +
                                     `📊 *Size:* ${fileSize ? formatBytes(fileSize) : 'N/A'}\n` +
                                     `> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: dlMek });

                        await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });

                    } catch (uploadErr) {
                        await socket.sendMessage(sender, {
                            text: `❌ *UPLOAD FAILED*\n\n` +
                                  `_${uploadErr.message}_\n\n` +
                                  `🔗 *Direct Link (ඔයාටම download කරගන්න):*\n${selected.link}`
                        }, { quoted: dlMek });
                        await socket.sendMessage(sender, { react: { text: '❌', key: dlMek.key } });
                    }
                };

                // session eka 5min timeout ekakin marenna kalin download listener eka alut karanawa (10 min)
                const curSess = global.cineverseSessions[sessionKey] || (global.cineverseSessions[sessionKey] = {});
                if (curSess.timeout) clearTimeout(curSess.timeout);
                curSess.listener = handleDownload;
                curSess.timeout = setTimeout(() => cleanup(), 600000);
                socket.ev.on('messages.upsert', handleDownload);

            } catch (infoErr) {
                cleanup();
                await socket.sendMessage(sender, { text: `❌ Info Error: ${infoErr.message}` }, { quoted: replyMek });
            }
        };

        global.cineverseSessions[sessionKey] = {
            listener: handleSelection,
            timeout: setTimeout(() => {
                cleanup();
            }, 300000) // 5 minutes
        };

        socket.ev.on('messages.upsert', handleSelection);

    } catch (err) {
        cleanup();
        await socket.sendMessage(sender, { text: `❌ Error: ${err.message}` }, { quoted: msg });
    }
    break;
}
// ==========================================
// LAKVISIONTV - SHAGGY XMD
// ==========================================
case 'lakvision':
case 'lv': {
    const DEFAULT_FOOTER = `\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;
    const TEMP_DIR = './tmp_lakvision';

    if (!args.length) {
        return socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 ❫*\n\n⚠️ *Invalid Usage!*\n\n🎬 *Example:*\n• .lakvision avatar\n• .lv game of thrones\n\n📝 _Please provide the Movie/Series name!_${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }

    const query = args.join(' ').trim();
    const API_BASE = "https://api.chamindu.site";
    const API_KEY = "chama_api_11230a80e5eed3c1b80bfcc5d1773ec9";
    const DEFAULT_IMAGE = "https://api.chamindu.site/logo.png";

    let lvSelectionListener = null;
    let lvDownloadListener = null;
    let lvMasterTimeout = null;

    const clearAllLvListeners = () => {
        if (lvSelectionListener) { socket.ev.off('messages.upsert', lvSelectionListener); lvSelectionListener = null; }
        if (lvDownloadListener)  { socket.ev.off('messages.upsert', lvDownloadListener);  lvDownloadListener  = null; }
        if (lvMasterTimeout)     { clearTimeout(lvMasterTimeout); lvMasterTimeout = null; }
    };

    const parseSizeMB = (s) => {
        if (!s) return 0;
        const m = s.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    const downloadToServer = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url, method: 'GET', responseType: 'stream',
            timeout: 0, maxRedirects: 5,
            maxContentLength: Infinity, maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://lakvisiontv.lk/',
                'Accept': '*/*'
            }
        });
        res.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });
    };

    await socket.sendMessage(sender, {
        text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗦𝗘𝗔𝗥𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🔍 *Searching LakvisionTV for:* _${query}_\n⚡ _Please wait..._`
    }, { quoted: msg });

    try {
        // ═══ SEARCH ═══
        const res = await axios.get(`${API_BASE}/api/v1/movie/lakvision/search?q=${encodeURIComponent(query)}&api_key=${API_KEY}`, { timeout: 120000 });
        const results = res.data.data || res.data.results || [];

        if (!results.length) {
            return socket.sendMessage(sender, {
                text: `*❪ 𝗦𝗛??𝗚𝗚𝗬 𝗫𝗠𝗗 ❫*\n\n😞 *No Results Found!*\n🎬 *Query:* _${query}_${DEFAULT_FOOTER}`
            }, { quoted: msg });
        }

        let listText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗟𝗔𝗞𝗩𝗜𝗦𝗜𝗢𝗡 ❫*\n\n🎯 *Query:* _${query}_\n📊 *Total:* _${results.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;
        results.slice(0, 15).forEach((item, index) => {
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            const typeIcon = (item.type === 'tvshows' || item.type === 'tv') ? '📺' : '🎥';
            listText += `*${num}* ➜ ${typeIcon} _${(item.title || 'Movie').substring(0, 32)}_ (${item.year || 'N/A'})\n`;
        });
        listText += `\n📌 _Reply with the number to download!_${DEFAULT_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, { text: listText }, { quoted: msg });
        const messageID = sentMsg.key.id;
        lvMasterTimeout = setTimeout(clearAllLvListeners, 180000);

        // ═══ USER PICKS MOVIE ═══
        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= results.length) {
                    return socket.sendMessage(sender, { text: `⚠️ *Invalid choice! Range: 01 - ${results.length}*${DEFAULT_FOOTER}` }, { quoted: replyMek });
                }

                if (lvSelectionListener) { socket.ev.off('messages.upsert', lvSelectionListener); lvSelectionListener = null; }

                const selectedItem = results[choice];
                const isTvShow = selectedItem.type === 'tvshows' || selectedItem.type === 'tv';

                await socket.sendMessage(sender, {
                    text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🎬 *Fetching details...*\n⚡ _Please wait..._`
                }, { quoted: replyMek });

                try {
                    const detailsRes = await axios.get(`${API_BASE}/api/v1/movie/lakvision/infodl?q=${encodeURIComponent(selectedItem.link || selectedItem.url)}&api_key=${API_KEY}`, { timeout: 120000 });
                    const movieInfo = detailsRes.data.data || {};
                    const validDownloads = movieInfo.downloads || [];
                    const episodes = movieInfo.episodes || [];

                    // Details
                    let detailsText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗟𝗔𝗞𝗩𝗜𝗦𝗜𝗢𝗡 ❫*\n\n`;
                    detailsText += `🎬 *${movieInfo.title || selectedItem.title}*\n`;
                    detailsText += `⭐ *IMDb:* ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n`;
                    detailsText += `📅 *Year:* ${movieInfo.year || 'N/A'}\n`;
                    if (movieInfo.duration) detailsText += `⏳ *Duration:* ${movieInfo.duration}\n`;
                    if (movieInfo.country) detailsText += `🌍 *Country:* ${movieInfo.country}\n`;
                    if (movieInfo.genres) detailsText += `🎭 *Genres:* ${Array.isArray(movieInfo.genres) ? movieInfo.genres.join(', ') : movieInfo.genres}\n`;
                    if (movieInfo.story) detailsText += `\n📝 *Story:* _${movieInfo.story.substring(0, 200)}..._\n`;
                    detailsText += DEFAULT_FOOTER;

                    await socket.sendMessage(sender, {
                        image: { url: movieInfo.image || selectedItem.image || DEFAULT_IMAGE },
                        caption: detailsText
                    }, { quoted: replyMek });

                    // TV Episodes
                    if (isTvShow && episodes.length > 0) {
                        let epListText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗘𝗣𝗜𝗦𝗢𝗗𝗘𝗦 ❫*\n\n📺 *Total Episodes:* ${episodes.length}\n\n`;
                        episodes.slice(0, 15).forEach((ep, epIdx) => {
                            epListText += `*${epIdx + 1}.* ${ep.name || ep.title || 'Episode ' + (epIdx + 1)}\n`;
                        });
                        epListText += `\n📌 _Reply with number to download._${DEFAULT_FOOTER}`;
                        await socket.sendMessage(sender, { text: epListText }, { quoted: replyMek });
                        return;
                    }

                    // Movie Downloads
                    if (validDownloads.length === 0) {
                        return socket.sendMessage(sender, { text: `⚠️ *No Direct Downloads available.*${DEFAULT_FOOTER}` }, { quoted: replyMek });
                    }

                    let dlText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗦 ❫*\n\n📥 *Select Quality:*\n\n`;
                    validDownloads.slice(0, 20).forEach((dl, i) => {
                        const num = (i + 1) < 10 ? `0${i + 1}` : `${i + 1}`;
                        const sizeMB = parseSizeMB(dl.size);
                        const note = sizeMB > 2000 ? ' ⚠️' : ' ✓';
                        dlText += `*${num}* ➜ 💾 _${dl.quality || 'HD'}_ (${dl.size || 'N/A'})${note}\n`;
                    });
                    dlText += `\n📌 _Reply with number to send file._${DEFAULT_FOOTER}`;

                    const dlSentMsg = await socket.sendMessage(sender, { text: dlText }, { quoted: replyMek });
                    const dlMessageID = dlSentMsg.key.id;

                    // ═══ DOWNLOAD HANDLER ═══
                    const handleDownloadSelection = async ({ messages: dlReplyMessages }) => {
                        const dlReplyMek = dlReplyMessages[0];
                        if (!dlReplyMek?.message || dlReplyMek.key.remoteJid !== sender) return;

                        const dlChoiceText = dlReplyMek.message.conversation || dlReplyMek.message.extendedTextMessage?.text;
                        if (dlReplyMek.message.extendedTextMessage?.contextInfo?.stanzaId !== dlMessageID) return;

                        const dlChoice = parseInt(dlChoiceText) - 1;
                        if (isNaN(dlChoice) || dlChoice < 0 || dlChoice >= validDownloads.length) {
                            return socket.sendMessage(sender, { text: `⚠️ *Invalid quality number!*` }, { quoted: dlReplyMek });
                        }

                        clearAllLvListeners();
                        const selectedDownload = validDownloads[dlChoice];
                        const fileUrl = selectedDownload.link || selectedDownload.download_link || selectedDownload.direct_link;
                        const sizeMB = parseSizeMB(selectedDownload.size);

                        await socket.sendMessage(sender, { react: { text: '📥', key: dlReplyMek.key } });

                        if (sizeMB > 2000) {
                            return socket.sendMessage(sender, {
                                text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${movieInfo.title || selectedItem.title}*\n📌 *${selectedDownload.quality}*\n📦 *${selectedDownload.size}*\n\n🔗 *Direct Link:*\n${fileUrl}\n\n_IDM එකෙන් download කරන්න._${DEFAULT_FOOTER}`
                            }, { quoted: dlReplyMek });
                        }

                        await socket.sendMessage(sender, {
                            text: `⏳ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚*\n\n📌 *${selectedDownload.quality}*\n📦 *Size:* ${selectedDownload.size || 'N/A'}\n\n_කරුණාකර රැඳී සිටින්න..._`
                        }, { quoted: dlReplyMek });

                        await fs.ensureDir(TEMP_DIR);
                        const safeName = (movieInfo.title || selectedItem.title).replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 50);
                        const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.mp4`);

                        try {
                            await downloadToServer(fileUrl, localFile);
                            const stats = await fs.stat(localFile);
                            const realSizeMB = stats.size / 1024 / 1024;

                            if (realSizeMB < 1) {
                                await fs.remove(localFile).catch(() => {});
                                throw new Error('Download failed — file too small');
                            }

                            await socket.sendMessage(sender, {
                                text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending..._`
                            }, { quoted: dlReplyMek });

                            try {
                                await socket.sendMessage(sender, {
                                    document: { url: localFile },
                                    mimetype: 'video/mp4',
                                    fileName: `${safeName} - ${selectedDownload.quality || 'HD'}.mp4`,
                                    caption: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗟𝗔𝗞𝗩𝗜𝗦𝗜𝗢𝗡*\n\n🎬 *Title:* ${movieInfo.title || selectedItem.title}\n📌 *Quality:* ${selectedDownload.quality || 'HD'}\n📦 *Size:* ${selectedDownload.size || 'N/A'}\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
                                }, { quoted: dlReplyMek });
                                await socket.sendMessage(sender, { react: { text: '✅', key: dlReplyMek.key } });
                            } catch (sendErr) {
                                await socket.sendMessage(sender, {
                                    text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${fileUrl}${DEFAULT_FOOTER}`
                                }, { quoted: dlReplyMek });
                            }
                            await fs.remove(localFile).catch(() => {});
                        } catch (downloadErr) {
                            await socket.sendMessage(sender, {
                                text: `❌ *Download Error:* _${downloadErr.message}_\n\n🔗 *Direct Link:*\n${fileUrl}${DEFAULT_FOOTER}`
                            }, { quoted: dlReplyMek });
                            try { await fs.remove(localFile); } catch {}
                        }
                    };

                    lvDownloadListener = handleDownloadSelection;
                    socket.ev.on('messages.upsert', lvDownloadListener);

                } catch (detailsErr) {
                    clearAllLvListeners();
                    await socket.sendMessage(sender, { text: `❌ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗘𝗥𝗥𝗢𝗥:* ${detailsErr.message}${DEFAULT_FOOTER}` }, { quoted: replyMek });
                }
            }
        };

        lvSelectionListener = handleSelection;
        socket.ev.on('messages.upsert', lvSelectionListener);

    } catch (err) {
        clearAllLvListeners();
        await socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗘𝗥𝗥𝗢𝗥 ❫*\n\n❌ *Search Error:* ${err.message}${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }
    break;
}
// ==========================================
// PIRATELK - SHAGGY XMD (Link Only - HTML page)
// ==========================================
case 'piratelk':
case 'plk': {
    const DEFAULT_FOOTER = `\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;

    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .piratelk pushpa*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const query = args.join(' ').trim();
    const API_BASE = "https://api.chamindu.site";
    const API_KEY = "chama_api_11230a80e5eed3c1b80bfcc5d1773ec9";
    const DEFAULT_IMAGE = "https://api.chamindu.site/logo.png";

    let plkSelectionListener = null;
    let plkDownloadListener = null;
    let plkMasterTimeout = null;

    const clearAllPlkListeners = () => {
        if (plkSelectionListener) { socket.ev.off('messages.upsert', plkSelectionListener); plkSelectionListener = null; }
        if (plkDownloadListener)  { socket.ev.off('messages.upsert', plkDownloadListener);  plkDownloadListener  = null; }
        if (plkMasterTimeout)     { clearTimeout(plkMasterTimeout); plkMasterTimeout = null; }
    };

    const parseSizeMB = (s) => {
        if (!s) return 0;
        const m = s.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    await socket.sendMessage(sender, {
        text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗦𝗘𝗔𝗥𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🔍 *Searching PirateLK for:* _${query}_\n⚡ _Please wait..._`
    }, { quoted: msg });

    try {
        // ═══ STEP 1 : SEARCH ═══
        const res = await axios.get(`${API_BASE}/api/v1/movie/piratelk/search?q=${encodeURIComponent(query)}&api_key=${API_KEY}`, {
            timeout: 120000
        });
        const results = res.data.data || res.data.results || [];

        if (!results.length) {
            await socket.sendMessage(sender, {
                text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 ❫*\n\n😞 *No Results Found on PirateLK!*\n🎬 *Query:* _${query}_${DEFAULT_FOOTER}`
            }, { quoted: msg });
            break;
        }

        let listText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗣𝗜𝗥𝗔𝗧𝗘𝗟𝗞 ❫*\n\n🎯 *Query:* _${query}_\n📊 *Total:* _${results.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;

        results.slice(0, 15).forEach((item, index) => {
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            const typeIcon = (item.type === 'tvshows' || item.type === 'tv') ? '📺' : '🎥';
            listText += `*${num}* ➜ ${typeIcon} _${(item.title || 'Movie').substring(0, 32)}_ (${item.year || 'N/A'})\n`;
        });

        listText += `\n📌 _Reply with the number to see download links!_${DEFAULT_FOOTER}`;
        const sentMsg = await socket.sendMessage(sender, { text: listText }, { quoted: msg });
        const messageID = sentMsg.key.id;

        plkMasterTimeout = setTimeout(clearAllPlkListeners, 180000);

        // ═══ STEP 2 : USER PICKS A MOVIE ═══
        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= results.length) {
                    return socket.sendMessage(sender, {
                        text: `⚠️ *Invalid choice! Range: 01 - ${results.length}*${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                }

                if (plkSelectionListener) { socket.ev.off('messages.upsert', plkSelectionListener); plkSelectionListener = null; }

                const selectedItem = results[choice];

                await socket.sendMessage(sender, {
                    text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🎬 *Fetching Movie details from PirateLK...*\n⚡ _Please wait..._`
                }, { quoted: replyMek });

                try {
                    // ═══ STEP 3 : INFO + DL ═══
                    const detailsRes = await axios.get(`${API_BASE}/api/v1/movie/piratelk/infodl?q=${encodeURIComponent(selectedItem.link || selectedItem.url)}&api_key=${API_KEY}`, {
                        timeout: 120000
                    });
                    const movieInfo = detailsRes.data.data || {};
                    const validDownloads = movieInfo.downloads || [];

                    // Details
                    let detailsText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗣𝗜𝗥𝗔𝗧𝗘𝗟𝗞 ❫*\n\n`;
                    detailsText += `🎬 *${movieInfo.title || selectedItem.title}*\n`;
                    detailsText += `⭐ *IMDb:* ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n`;
                    detailsText += `📅 *Year:* ${movieInfo.year || 'N/A'}\n`;
                    if (movieInfo.duration) detailsText += `⏳ *Duration:* ${movieInfo.duration}\n`;
                    if (movieInfo.country) detailsText += `🌍 *Country:* ${movieInfo.country}\n`;
                    if (movieInfo.genres) detailsText += `🎭 *Genres:* ${Array.isArray(movieInfo.genres) ? movieInfo.genres.join(', ') : movieInfo.genres}\n`;
                    if (movieInfo.story) detailsText += `\n📝 *Story:* _${movieInfo.story.substring(0, 220)}..._\n`;
                    detailsText += DEFAULT_FOOTER;

                    const posterUrl = movieInfo.image || selectedItem.image || DEFAULT_IMAGE;
                    await socket.sendMessage(sender, {
                        image: { url: posterUrl },
                        caption: detailsText
                    }, { quoted: replyMek });

                    if (validDownloads.length === 0) {
                        return socket.sendMessage(sender, {
                            text: `⚠️ *No Direct Downloads available for this movie right now.*${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                    }

                    // ═══ STEP 4 : DOWNLOAD OPTIONS ═══
                    let dlText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗦 ❫*\n\n📥 *Select Quality:*\n\n`;
                    validDownloads.slice(0, 20).forEach((dl, i) => {
                        const num = (i + 1) < 10 ? `0${i + 1}` : `${i + 1}`;
                        // ⭐ Fix: title → quality → name → "Download N"
                        const label = dl.title || dl.quality || dl.name || `Download ${i + 1}`;
                        const size = dl.size || dl.file_size || 'N/A';
                        dlText += `*${num}* ➜ 💾 _${label}_ (${size})\n`;
                    });
                    dlText += `\n📌 _Reply with number to get download link._${DEFAULT_FOOTER}`;

                    const dlSentMsg = await socket.sendMessage(sender, { text: dlText }, { quoted: replyMek });
                    const dlMessageID = dlSentMsg.key.id;

                    // ═══ STEP 5 : USER PICKS DOWNLOAD ═══
                    const handleDownloadSelection = async ({ messages: dlReplyMessages }) => {
                        const dlReplyMek = dlReplyMessages[0];
                        if (!dlReplyMek?.message || dlReplyMek.key.remoteJid !== sender) return;

                        const dlChoiceText = dlReplyMek.message.conversation || dlReplyMek.message.extendedTextMessage?.text;
                        if (dlReplyMek.message.extendedTextMessage?.contextInfo?.stanzaId !== dlMessageID) return;

                        const dlChoice = parseInt(dlChoiceText) - 1;
                        if (isNaN(dlChoice) || dlChoice < 0 || dlChoice >= validDownloads.length) {
                            return socket.sendMessage(sender, { text: `⚠️ *Invalid quality number!*` }, { quoted: dlReplyMek });
                        }

                        clearAllPlkListeners();
                        const selectedDownload = validDownloads[dlChoice];
                        const fileUrl = selectedDownload.link || selectedDownload.download_link || selectedDownload.direct_link;
                        const label = selectedDownload.title || selectedDownload.quality || selectedDownload.name || `Download ${dlChoice + 1}`;
                        const size = selectedDownload.size || selectedDownload.file_size || 'N/A';

                        await socket.sendMessage(sender, { react: { text: '🔗', key: dlReplyMek.key } });

                        // ⚠️ PirateLK — HTML page → link only
                        await socket.sendMessage(sender, {
                            text:
`✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗟𝗜𝗡𝗞*

🎬 *Title:* ${movieInfo.title || selectedItem.title}
📌 *Quality:* ${label}
📦 *Size:* ${size}

🔗 *Download Link:*
${fileUrl}

💡 *Tips:*
• Browser එකෙන් open කරන්න
• IDM / ADM use කරන්න
• WiFi use කරන්න

> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
                        }, { quoted: dlReplyMek });

                        await socket.sendMessage(sender, { react: { text: '✅', key: dlReplyMek.key } });
                    };

                    plkDownloadListener = handleDownloadSelection;
                    socket.ev.on('messages.upsert', plkDownloadListener);

                } catch (detailsErr) {
                    clearAllPlkListeners();
                    await socket.sendMessage(sender, {
                        text: `❌ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗘𝗥𝗥𝗢𝗥:* ${detailsErr.message}${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                }
            }
        };

        plkSelectionListener = handleSelection;
        socket.ev.on('messages.upsert', plkSelectionListener);

    } catch (err) {
        clearAllPlkListeners();
        await socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗘𝗥𝗥𝗢𝗥 ❫*\n\n❌ *Search Error:* ${err.message}${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }
    break;
}
// ==========================================
// MEDIAFIRE - SHAGGY XMD File Downloader
// ==========================================
case 'mediafire':
case 'mfire':
case 'mf': {
    const MF_TEMP = './tmp_mediafire';
    const MF_MAX_MB = 1900; // WhatsApp document limit is ~2GB
    const MF_PREFIX = sessionConfig.PREFIX || config.PREFIX || '.';
    const MF_FOOTER = `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;
    const MF_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

    const mfUrl = (args.join(' ').match(/https?:\/\/(?:www\.)?mediafire\.com\/[^\s]+/i) || [])[0];

    if (!mfUrl) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '📁 MEDIAFIRE DOWNLOADER',
                `*MediaFire link ekak denna!*\n\n*📌 Usage:* \`${MF_PREFIX}mediafire <link>\`\n\n*Example:*\n\`${MF_PREFIX}mf https://www.mediafire.com/file/xxxxxxxx/file.zip/file\``,
                MF_FOOTER
            )
        }, { quoted: msg });
        break;
    }

    const mfDecode = (s) => String(s)
        .replace(/&amp;/g, '&').replace(/&#0*38;/g, '&')
        .replace(/&#x2F;/gi, '/').replace(/&#0*47;/g, '/')
        .replace(/&quot;/g, '"').replace(/&#0*39;/g, "'");

    const mfMime = {
        mp4: 'video/mp4', mkv: 'video/x-matroska', avi: 'video/x-msvideo', webm: 'video/webm',
        mp3: 'audio/mpeg', m4a: 'audio/mp4', wav: 'audio/wav', ogg: 'audio/ogg',
        zip: 'application/zip', rar: 'application/vnd.rar', '7z': 'application/x-7z-compressed',
        apk: 'application/vnd.android.package-archive', pdf: 'application/pdf',
        jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', gif: 'image/gif', webp: 'image/webp',
        txt: 'text/plain', json: 'application/json',
        doc: 'application/msword',
        docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        pptx: 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        iso: 'application/x-iso9660-image'
    };

    let mfLocal = null;

    try {
        await socket.sendMessage(sender, { react: { text: '⏳', key: msg.key } });

        // ── Step 1: open the MediaFire page ──
        const page = await axios.get(mfUrl, {
            responseType: 'text',
            timeout: 120000,
            maxRedirects: 5,
            maxContentLength: 5 * 1024 * 1024,
            headers: {
                'User-Agent': MF_UA,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9'
            },
            validateStatus: () => true
        });
        const html = typeof page.data === 'string' ? page.data : '';

        // ── Step 2: find the real download link ──
        let dl = null;

        // newer layout: base64 "scrambled" url on the download button
        const scr = html.match(/data-scrambled-url=["']([^"']+)["']/i);
        if (scr) {
            try { dl = Buffer.from(scr[1], 'base64').toString('utf8'); } catch {}
        }
        // older layout: plain href to downloadXXXX.mediafire.com
        if (!dl || !/^https?:\/\//i.test(dl)) {
            const m = html.match(/href=["'](https?:\/\/download[0-9]*\.mediafire\.com\/[^"']+)["']/i)
                   || html.match(/id=["']downloadButton["'][^>]*href=["'](https?:\/\/[^"']+)["']/i)
                   || html.match(/aria-label=["']Download file["'][^>]*href=["'](https?:\/\/[^"']+)["']/i);
            if (m) dl = m[1];
        }
        if (dl) dl = mfDecode(dl).trim();

        if (!dl || !/^https?:\/\//i.test(dl)) {
            throw new Error('Download link eka hoyaganna ba (file eka delete wela / private / MediaFire layout eka wenas wela)');
        }

        // ── Step 3: file info ──
        let mfName = (html.match(/<div[^>]*class=["']filename["'][^>]*>\s*([^<]+?)\s*<\/div>/i) || [])[1];
        if (!mfName) {
            try { mfName = decodeURIComponent(new URL(dl).pathname.split('/').pop()); } catch {}
        }
        mfName = mfDecode(mfName || 'mediafire_file').replace(/[\\/:*?"<>|]/g, '_').trim() || 'mediafire_file';

        const mfSizeStr = (html.match(/Download\s*\(\s*([\d.,]+\s*[KMGT]?B)\s*\)/i) || [])[1] || 'Unknown';
        const mfExt = path.extname(mfName).slice(1).toLowerCase();
        const mfMimetype = mfMime[mfExt] || 'application/octet-stream';

        if (parseSizeMB(mfSizeStr) > MF_MAX_MB) {
            await socket.sendMessage(sender, {
                text: `⚠️ *File eka loku wadi!* (${mfSizeStr})\nWhatsApp ekata yawanna ba.\n\n🔗 *Direct link:*\n${dl}\n\n> ${MF_FOOTER}`
            }, { quoted: msg });
            break;
        }

        await socket.sendMessage(sender, {
            text: formatMessage(
                '📁 MEDIAFIRE DOWNLOADER',
                `📄 *Name :* ${mfName}\n📦 *Size :* ${mfSizeStr}\n📎 *Type :* ${mfExt ? mfExt.toUpperCase() : 'FILE'}\n\n⏳ _Downloading... karunakara randi sitinna_`,
                MF_FOOTER
            )
        }, { quoted: msg });

        // ── Step 4: stream to disk ──
        await fs.ensureDir(MF_TEMP);
        mfLocal = path.join(MF_TEMP, `${Date.now()}_${mfName}`);

        const dlRes = await axios({
            url: dl,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 10,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: { 'User-Agent': MF_UA, 'Referer': 'https://www.mediafire.com/', 'Accept': '*/*' }
        });

        const ct = (dlRes.headers['content-type'] || '').toLowerCase();
        if (ct.includes('text/html')) {
            dlRes.data.destroy();
            throw new Error('MediaFire file ekata wenuwata web page ekak dunna (link expire wela wenna puluwan)');
        }

        const declaredMB = parseInt(dlRes.headers['content-length'] || '0', 10) / 1024 / 1024;
        if (declaredMB > MF_MAX_MB) {
            dlRes.data.destroy();
            await socket.sendMessage(sender, {
                text: `⚠️ *File eka loku wadi!* (${declaredMB.toFixed(0)} MB)\n\n🔗 *Direct link:*\n${dl}\n\n> ${MF_FOOTER}`
            }, { quoted: msg });
            break;
        }

        await pipeline(dlRes.data, fs.createWriteStream(mfLocal));

        const st = await fs.stat(mfLocal);
        if (!st.size) throw new Error('Downloaded file eka empty');
        const realMB = (st.size / 1024 / 1024).toFixed(2);

        // ── Step 5: send to WhatsApp ──
        await socket.sendMessage(sender, {
            document: { url: mfLocal },
            mimetype: mfMimetype,
            fileName: mfName,
            caption: `✅ *𝗠𝗘𝗗𝗜𝗔𝗙𝗜𝗥𝗘 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗘𝗥*\n\n📄 *Name :* ${mfName}\n📦 *Size :* ${realMB} MB\n\n> ${MF_FOOTER}`
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

    } catch (err) {
        console.error('[MediaFire] Error:', err.message);
        await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } }).catch(() => {});
        await socket.sendMessage(sender, {
            text: `❌ *MediaFire Error*\n\n_${err.message}_\n\n🔗 *Link:* ${mfUrl}\n💡 Browser ekakin open karala balanna.\n\n> ${MF_FOOTER}`
        }, { quoted: msg });
    } finally {
        if (mfLocal) await fs.remove(mfLocal).catch(() => {});
    }
    break;
}

case 'gdrive':
case 'gdl': {
    const DEFAULT_FOOTER = `\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;
    const TEMP_DIR = './tmp_gdrive';

    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර Google Drive link එක ලබාදෙන්න!*\n\n*📌 Usage:* `.gdrive <drive-link>`\n\n*Example:*\n`.gdrive https://drive.google.com/file/d/1pZ2rqEVkUp8g190uvLgjyrnHFk5qzlo5/view`',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const inputUrl = args.join(' ').trim();

    // ⭐ Validate Google Drive URL
    if (!inputUrl.includes('drive.google.com') && 
        !inputUrl.includes('drive.usercontent.google.com') && 
        !inputUrl.includes('docs.google.com')) {
        return socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ INVALID URL',
                '*කරුණාකර වලංගු Google Drive link එකක් ලබාදෙන්න!*\n\n*📌 Supported:*\n• `drive.google.com/file/d/ID/view`\n• `docs.google.com/uc?export=download&id=ID`',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }

    // ⭐ Extract file ID
    const idMatch = inputUrl.match(/(?:id=|\/d\/|file\/d\/)([a-zA-Z0-9_-]+)/);
    if (!idMatch || !idMatch[1]) {
        return socket.sendMessage(sender, {
            text: `❌ *File ID එක extract කළ නොහැක!*\n\n*Provided:* _${inputUrl.substring(0, 80)}..._${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }

    const fileId = idMatch[1];
    console.log(`[GDrive-CMD] File ID: ${fileId}`);

    // ⭐ Helper: Parse size
    const parseSizeMB = (s) => {
        if (!s) return 0;
        const m = s.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    // ⭐ Helper: Download Google Drive with cookie bypass
    const downloadGdriveFile = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));

        const cookieJar = new Map();
        const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

        const addCookies = (setCookies = []) => {
            setCookies.forEach(c => {
                const [nameVal] = c.split(';');
                const [name, ...valParts] = nameVal.split('=');
                cookieJar.set(name.trim(), valParts.join('=').trim());
            });
        };

        const cookieHeader = () => Array.from(cookieJar.entries()).map(([k, v]) => `${k}=${v}`).join('; ');

        try {
            // Step 1: Get HTML page
            console.log(`[GDrive] Step 1: Fetching HTML...`);

            const firstRes = await axios.get(
                `https://drive.usercontent.google.com/download?id=${fileId}&export=download&authuser=0`,
                {
                    responseType: 'text',
                    timeout: 120000,
                    maxRedirects: 5,
                    headers: {
                        'User-Agent': UA,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9'
                    },
                    validateStatus: () => true
                }
            );

            addCookies(firstRes.headers['set-cookie']);

            const html = firstRes.data || '';
            console.log(`[GDrive] HTML length: ${html.length}`);

            // Step 2: Extract form action + inputs
            const actionMatch = html.match(/<form[^>]+action="([^"]+)"/);
            const formAction = actionMatch ? actionMatch[1].replace(/&amp;/g, '&') : null;

            const inputs = {};
            let m;

            const regex1 = /<input[^>]+name="([^"]+)"[^>]+value="([^"]*)"/g;
            while ((m = regex1.exec(html)) !== null) inputs[m[1]] = m[2];

            const regex2 = /<input[^>]+value="([^"]*)"[^>]+name="([^"]+)"/g;
            while ((m = regex2.exec(html)) !== null) inputs[m[2]] = m[1];

            console.log(`[GDrive] Form: ${formAction ? 'found' : 'none'}, Inputs: ${JSON.stringify(Object.keys(inputs))}`);

            // Step 3: Build download URL
            let downloadUrl = '';
            if (formAction) {
                const params = new URLSearchParams();
                for (const [k, v] of Object.entries(inputs)) params.append(k, v);
                downloadUrl = `${formAction}?${params.toString()}`;
            } else {
                const params = new URLSearchParams({
                    id: fileId,
                    export: 'download',
                    confirm: 't'
                });
                if (inputs.uuid) params.append('uuid', inputs.uuid);
                if (inputs.at) params.append('at', inputs.at);
                downloadUrl = `https://drive.usercontent.google.com/download?${params.toString()}`;
            }

            console.log(`[GDrive] Step 2: Download URL: ${downloadUrl.substring(0, 120)}...`);

            // Step 4: Download the file
            const writer = fs.createWriteStream(dest);
            const fileRes = await axios({
                url: downloadUrl,
                method: 'GET',
                responseType: 'stream',
                timeout: 0,
                maxRedirects: 10,
                maxContentLength: Infinity,
                maxBodyLength: Infinity,
                headers: {
                    'User-Agent': UA,
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cookie': cookieHeader(),
                    'Referer': `https://drive.usercontent.google.com/download?id=${fileId}&export=download`,
                    'Origin': 'https://drive.usercontent.google.com'
                }
            });

            const ct = (fileRes.headers['content-type'] || '').toLowerCase();
            const cl = fileRes.headers['content-length'] || '0';
            console.log(`[GDrive] Content-Type: ${ct}, Length: ${cl}`);

            if (ct.includes('text/html')) {
                fileRes.data.destroy();
                throw new Error('Google Drive returned HTML (blocked/quota)');
            }

            fileRes.data.pipe(writer);
            await new Promise((resolve, reject) => {
                writer.on('finish', resolve);
                writer.on('error', reject);
                fileRes.data.on('error', reject);
            });

            const stats = await fs.stat(dest);
            const sizeMB = stats.size / 1024 / 1024;
            console.log(`[GDrive] ✅ Downloaded: ${sizeMB.toFixed(1)} MB`);

            if (sizeMB < 1) {
                await fs.remove(dest).catch(() => {});
                throw new Error(`File too small: ${sizeMB.toFixed(2)} MB`);
            }

            return { success: true, size: stats.size };

        } catch (err) {
            console.error('[GDrive] ❌ Error:', err.message);
            throw err;
        }
    };

    await socket.sendMessage(sender, {
        text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗚𝗗𝗥𝗜𝗩𝗘 ❫*\n\n⏳ *Fetching file details...*\n📌 *File ID:* \`${fileId.substring(0, 20)}...\`\n\n_කරුණාකර රැඳී සිටින්න..._`
    }, { quoted: msg });

    let localFile = null;
    try {
        // ⭐ Server download
        await fs.ensureDir(TEMP_DIR);
        localFile = path.join(TEMP_DIR, `gdrive_${fileId}_${Date.now()}`);

        await downloadGdriveFile(inputUrl, localFile);

        const stats = await fs.stat(localFile);
        const realSizeMB = stats.size / 1024 / 1024;

        await socket.sendMessage(sender, {
            text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
        }, { quoted: msg });

        // ⭐ Send as document
        const fileName = `GDrive_${fileId.substring(0, 10)}_${Date.now()}.mp4`;

        try {
            await socket.sendMessage(sender, {
                document: { url: localFile },
                mimetype: 'video/mp4',
                fileName: fileName,
                caption: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗚𝗗𝗥𝗜𝗩𝗘*\n\n📦 *Size:* ${realSizeMB.toFixed(1)} MB\n🔗 *File ID:* \`${fileId.substring(0, 15)}...\`\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
            }, { quoted: msg });

            await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

        } catch (sendErr) {
            // Send fail → link only
            await socket.sendMessage(sender, {
                text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${inputUrl}${DEFAULT_FOOTER}`
            }, { quoted: msg });
        }

        // Cleanup
        await fs.remove(localFile).catch(() => {});

    } catch (err) {
        console.error('[GDrive-CMD] Error:', err.message);

        // ⭐ Fallback: direct link with Google Drive format
        const directUrl = `https://drive.usercontent.google.com/download?id=${fileId}&export=download&confirm=t`;

        await socket.sendMessage(sender, {
            text:
`⚠️ *Direct Download*

📦 *File ID:* \`${fileId.substring(0, 15)}...\`

🔗 *Download Link:*
${directUrl}

💡 *Tips:*
• Browser එකෙන් open කරන්න
• IDM / ADM use කරන්න

> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
        }, { quoted: msg });

        if (localFile) { try { await fs.remove(localFile); } catch {} }
    }

    break;
}

// ==========================================
// YOUTUBE - SHAGGY XMD Video/Audio Downloader
// ==========================================
case 'youtube':
case 'yt':
case 'ytmp3': {
    const DEFAULT_FOOTER = `\n\n> 📥 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗠𝗘𝗗𝗜𝗔 📥\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;
    const TEMP_DIR = './tmp_youtube';

    if (!args.length) {
        return socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 ❫*\n\n⚠️ *Invalid Usage!*\n\n🎬 *Example:*\n• .youtube https://youtu.be/xxxx\n• .ytmp3 https://youtu.be/xxxx\n\n📝 _Please provide a YouTube URL!_${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }

    // ⭐ Determine if audio or video
    const commandUsed = msg.message?.conversation?.split(' ')[0]?.replace('.', '').toLowerCase()
        || msg.message?.extendedTextMessage?.text?.split(' ')[0]?.replace('.', '').toLowerCase()
        || 'youtube';
    const isAudio = commandUsed === 'ytmp3' || args.includes('mp3') || args.includes('audio');
    const dlType = isAudio ? 'mp3' : 'video';
    const url = args.find(a => a.startsWith('http')) || args[0];

    // ⭐ Validate URL
    if (!url.includes('youtu.be') && !url.includes('youtube.com')) {
        return socket.sendMessage(sender, {
            text: `❌ *Invalid YouTube URL!*\n\n🎬 *Provided:* _${url}_\n\n📝 _Use: .youtube <youtube-url>_${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }

    const API_BASE = "https://api.chamindu.site";
    const API_KEY = "chama_api_11230a80e5eed3c1b80bfcc5d1773ec9";

    // ⭐ Server download
    const downloadToServer = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 5,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://www.youtube.com/',
                'Accept': '*/*'
            }
        });
        res.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });
    };

    await socket.sendMessage(sender, {
        text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗖𝗢𝗡𝗩𝗘𝗥𝗧𝗜𝗡𝗚 ❫*\n\n⚡ *Fetching YouTube ${dlType.toUpperCase()}...*\n⏳ _Please wait a moment..._`
    }, { quoted: msg });

    try {
        // ═══ STEP 1 : FETCH YOUTUBE DATA ═══
        const res = await axios.get(`${API_BASE}/api/v1/download/youtube?url=${encodeURIComponent(url)}&type=${dlType}&api_key=${API_KEY}`, {
            timeout: 120000
        });
        const ytData = res.data.data;

        if (!ytData || !ytData.download_url) {
            return socket.sendMessage(sender, {
                text: `❌ *Failed to extract YouTube ${dlType.toUpperCase()}!*${DEFAULT_FOOTER}`
            }, { quoted: msg });
        }

        const title = (ytData.title || 'YouTube_Download').replace(/[^a-zA-Z0-9 _-]/g, '').trim().substring(0, 80);

        // ⭐ Server download
        await fs.ensureDir(TEMP_DIR);
        const safeName = title.replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 60);
        const fileExt = isAudio ? 'mp3' : 'mp4';
        const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.${fileExt}`);

        await socket.sendMessage(sender, {
            text: `⏳ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚*\n\n📌 *Title:* _${title.substring(0, 50)}..._\n⏳ *Duration:* ${ytData.duration || 'N/A'}\n\n_කරුණාකර රැඳී සිටින්න..._`
        }, { quoted: msg });

        try {
            await downloadToServer(ytData.download_url, localFile);
            const stats = await fs.stat(localFile);
            const realSizeMB = stats.size / 1024 / 1024;

            // ⚠️ Error page check
            if (realSizeMB < 0.1) {
                await fs.remove(localFile).catch(() => {});
                throw new Error('Download failed — file too small (error page detected)');
            }

            await socket.sendMessage(sender, {
                text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
            }, { quoted: msg });

            // ⭐ Send as Audio
            if (isAudio) {
                try {
                    await socket.sendMessage(sender, {
                        audio: { url: localFile },
                        mimetype: 'audio/mpeg',
                        fileName: `${safeName}.mp3`,
                        ptt: false
                    }, { quoted: msg });

                    await socket.sendMessage(sender, {
                        text: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗔𝗨𝗗𝗜𝗢*\n\n🎵 *Title:* ${title}\n⏳ *Duration:* ${ytData.duration || 'N/A'}\n📦 *Size:* ${realSizeMB.toFixed(1)} MB\n> 📥 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗠𝗘𝗗𝗜𝗔 📥`
                    }, { quoted: msg });

                } catch (sendErr) {
                    await socket.sendMessage(sender, {
                        text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${ytData.download_url}${DEFAULT_FOOTER}`
                    }, { quoted: msg });
                }
            }
            // ⭐ Send as Video
            else {
                try {
                    await socket.sendMessage(sender, {
                        video: { url: localFile },
                        mimetype: 'video/mp4',
                        fileName: `${safeName}.mp4`,
                        caption: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗩𝗜𝗗𝗘𝗢*\n\n🎬 *Title:* ${title}\n⏳ *Duration:* ${ytData.duration || 'N/A'}\n📦 *Size:* ${realSizeMB.toFixed(1)} MB\n> 📥 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗠𝗘𝗗𝗜𝗔 📥`
                    }, { quoted: msg });

                } catch (sendErr) {
                    await socket.sendMessage(sender, {
                        text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${ytData.download_url}${DEFAULT_FOOTER}`
                    }, { quoted: msg });
                }
            }

            // ⭐ Cleanup
            await fs.remove(localFile).catch(() => {});

        } catch (downloadErr) {
            console.error('[YouTube] download error:', downloadErr.message);
            await socket.sendMessage(sender, {
                text: `❌ *Download Error:* _${downloadErr.message}_\n\n🔗 *Direct Link:*\n${ytData.download_url}${DEFAULT_FOOTER}`
            }, { quoted: msg });
            try { await fs.remove(localFile); } catch {}
        }

    } catch (err) {
        await socket.sendMessage(sender, {
            text: `❌ *YouTube Error:* ${err.message}${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }
    break;
}
// ==========================================
// ANIMEXIN - SHAGGY XMD Donghua & Anime
// ==========================================
case 'animexin':
case 'donghua':
case 'ax': {
    const DEFAULT_FOOTER = `\n\n> 🐉 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗗𝗢𝗡𝗚𝗛𝗨𝗔 🐉\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;
    const TEMP_DIR = './tmp_animexin';

    if (!args.length) {
        return socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 ❫*\n\n⚠️ *Invalid Usage!*\n\n🎬 *Example:*\n• .animexin mortal\n• .donghua soul land\n• .ax renegade immortal\n\n📝 _Please provide Donghua / Anime name!_${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }

    const query = args.join(' ').trim();
    const API_BASE = "https://api.chamindu.site";
    const API_KEY = "chama_api_11230a80e5eed3c1b80bfcc5d1773ec9";
    const DEFAULT_IMAGE = "https://api.chamindu.site/logo.png";

    let axSelectionListener = null;
    let axDownloadListener = null;
    let axEpisodeListener = null;
    let axMasterTimeout = null;

    const clearAllAxListeners = () => {
        if (axSelectionListener) { socket.ev.off('messages.upsert', axSelectionListener); axSelectionListener = null; }
        if (axDownloadListener)  { socket.ev.off('messages.upsert', axDownloadListener);  axDownloadListener  = null; }
        if (axEpisodeListener)   { socket.ev.off('messages.upsert', axEpisodeListener);   axEpisodeListener   = null; }
        if (axMasterTimeout)     { clearTimeout(axMasterTimeout); axMasterTimeout = null; }
    };

    const parseSizeMB = (s) => {
        if (!s) return 0;
        const m = s.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    // ⭐ Server download
    const downloadToServer = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 5,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://animexin.vip/',
                'Accept': '*/*'
            }
        });
        res.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });
    };

    await socket.sendMessage(sender, {
        text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗦𝗘𝗔𝗥𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🔍 *Searching Animexin for:* _${query}_\n⚡ _Please wait..._`
    }, { quoted: msg });

    try {
        // ═══ STEP 1 : SEARCH ═══
        const res = await axios.get(`${API_BASE}/api/v1/anime/animexin/search?q=${encodeURIComponent(query)}&api_key=${API_KEY}`, {
            timeout: 120000
        });
        const results = res.data.data || [];

        if (!results.length) {
            return socket.sendMessage(sender, {
                text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 ❫*\n\n😢 *No Results Found on Animexin!*\n🎬 *Query:* _${query}_${DEFAULT_FOOTER}`
            }, { quoted: msg });
        }

        let listText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗔𝗡𝗜𝗠𝗘𝗫𝗜𝗡 ❫*\n\n🎯 *Query:* _${query}_\n📊 *Total:* _${results.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;
        results.slice(0, 15).forEach((item, index) => {
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            listText += `*${num}* ➔ 🐉 _${(item.title || 'Anime').substring(0, 35)}_ (${item.type || item.status || 'Donghua'})\n`;
        });
        listText += `\n📌 _Reply with the number to fetch details & downloads!_${DEFAULT_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, { text: listText }, { quoted: msg });
        const messageID = sentMsg.key.id;
        axMasterTimeout = setTimeout(clearAllAxListeners, 180000);

        // ═══ STEP 2 : USER PICKS ═══
        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= results.length) {
                    return socket.sendMessage(sender, {
                        text: `⚠️ *Invalid choice! Range: 01 - ${results.length}*${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                }

                if (axSelectionListener) { socket.ev.off('messages.upsert', axSelectionListener); axSelectionListener = null; }

                const selectedItem = results[choice];

                await socket.sendMessage(sender, {
                    text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🐉 *Fetching details and download links...*\n⚡ _Please wait..._`
                }, { quoted: replyMek });

                try {
                    // ═══ STEP 3 : INFO + DL ═══
                    const detailsRes = await axios.get(`${API_BASE}/api/v1/anime/animexin/infodl?q=${encodeURIComponent(selectedItem.url || selectedItem.link)}&api_key=${API_KEY}`, {
                        timeout: 120000
                    });
                    const animeInfo = detailsRes.data.data || {};
                    const validDownloads = animeInfo.downloads || [];
                    const episodes = animeInfo.episodes || [];

                    // Details
                    let detailsText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗡𝗚𝗛𝗨𝗔 ❫*\n\n`;
                    detailsText += `🎬 *${animeInfo.title || selectedItem.title}*\n`;
                    detailsText += `📡 *Status:* ${animeInfo.status || 'Ongoing'}\n`;
                    if (animeInfo.studio) detailsText += `🏢 *Studio:* ${animeInfo.studio}\n`;
                    if (animeInfo.country) detailsText += `🌍 *Country:* ${animeInfo.country}\n`;
                    if (animeInfo.genres) detailsText += `🎭 *Genres:* ${Array.isArray(animeInfo.genres) ? animeInfo.genres.join(', ') : animeInfo.genres}\n`;
                    if (animeInfo.synopsis) detailsText += `\n📝 *Story:* _${animeInfo.synopsis.substring(0, 200)}..._\n`;
                    detailsText += DEFAULT_FOOTER;

                    const posterUrl = animeInfo.poster || animeInfo.image || selectedItem.image || DEFAULT_IMAGE;
                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: posterUrl },
                        caption: detailsText
                    }, { quoted: replyMek });

                    const infoMsgID = infoMsg.key.id;

                    // ═══ STEP 4 : DOWNLOADS ═══
                    if (validDownloads.length > 0) {
                        let dlText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗦 ❫*\n\n`;
                        validDownloads.slice(0, 20).forEach((dl, i) => {
                            const num = (i + 1) < 10 ? `0${i + 1}` : `${i + 1}`;
                            const isTg = (dl.url || '').includes('t.me/');
                            const note = isTg ? ' 🔗' : ' ✓';
                            dlText += `*${num}* ➔ 💾 _${dl.server || 'Direct'}_ [${dl.language || 'Sub'}] (${dl.quality || 'HD'})${note}\n`;
                        });
                        dlText += `\n*👇 SELECT A NUMBER 👇*\n\n📌 _✓ = Document • 🔗 = Telegram_${DEFAULT_FOOTER}`;

                        const dlMsg = await socket.sendMessage(sender, { text: dlText }, { quoted: infoMsg });
                        const dlMsgID = dlMsg.key.id;

                        // ═══ STEP 5 : USER PICKS DOWNLOAD ═══
                        const handleDownload = async ({ messages: dlMsgs }) => {
                            const dlMek = dlMsgs?.[0];
                            if (!dlMek?.message || dlMek.key.remoteJid !== sender) return;

                            const dlChoiceText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                            if (dlMek.message.extendedTextMessage?.contextInfo?.stanzaId !== dlMsgID) return;

                            const dlIdx = parseInt(dlChoiceText) - 1;
                            if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= validDownloads.length) {
                                return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${validDownloads.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: dlMek });
                            }

                            clearAllAxListeners();
                            const selectedDl = validDownloads[dlIdx];
                            const dlUrl = selectedDl.url || selectedDl.direct_download;
                            const isTelegram = dlUrl.includes('t.me/');

                            await socket.sendMessage(sender, { react: { text: '📥', key: dlMek.key } });

                            // 🔗 Telegram
                            if (isTelegram) {
                                return socket.sendMessage(sender, {
                                    text: `📱 *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗧𝗘𝗟𝗘𝗚𝗥𝗔𝗠*\n\n🎬 *${animeInfo.title || selectedItem.title}*\n📌 *${selectedDl.quality || 'HD'}*\n\n🔗 *Telegram Link:*\n${dlUrl}\n\n_Telegram bot එකෙන් download කරන්න._${DEFAULT_FOOTER}`
                                }, { quoted: dlMek });
                            }

                            // ⚠️ 2GB limit
                            const sizeMB = parseSizeMB(selectedDl.size);
                            if (sizeMB > 2000) {
                                return socket.sendMessage(sender, {
                                    text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${animeInfo.title || selectedItem.title}*\n📌 *${selectedDl.quality}*\n📦 *${selectedDl.size}*\n\n🔗 *Direct Link:*\n${dlUrl}\n\n_IDM එකෙන් download කරන්න._${DEFAULT_FOOTER}`
                                }, { quoted: dlMek });
                            }

                            await socket.sendMessage(sender, {
                                text: `⏳ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚*\n\n📌 *${selectedDl.quality || 'HD'}*\n📦 *Size:* ${selectedDl.size || 'N/A'}\n📡 *Server:* ${selectedDl.server || 'Direct'}\n\n_කරුණාකර රැඳී සිටින්න..._`
                            }, { quoted: dlMek });

                            // ⭐ Server download
                            await fs.ensureDir(TEMP_DIR);
                            const safeName = (animeInfo.title || selectedItem.title).replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 50);
                            const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.mp4`);

                            try {
                                await downloadToServer(dlUrl, localFile);
                                const stats = await fs.stat(localFile);
                                const realSizeMB = stats.size / 1024 / 1024;

                                if (realSizeMB < 1) {
                                    await fs.remove(localFile).catch(() => {});
                                    throw new Error('Download failed — file too small (error page detected)');
                                }

                                await socket.sendMessage(sender, {
                                    text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                                }, { quoted: dlMek });

                                try {
                                    await socket.sendMessage(sender, {
                                        document: { url: localFile },
                                        mimetype: 'video/mp4',
                                        fileName: `${safeName} - ${selectedDl.quality || 'HD'}.mp4`,
                                        caption: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗡𝗚𝗛𝗨𝗔*\n\n🎬 *Title:* ${animeInfo.title || selectedItem.title}\n📡 *Status:* ${animeInfo.status || 'Ongoing'}\n📌 *Quality:* ${selectedDl.quality || 'HD'}\n📦 *Size:* ${selectedDl.size || 'N/A'}\n> 🐉 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🐉`
                                    }, { quoted: dlMek });

                                    await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });

                                } catch (sendErr) {
                                    await socket.sendMessage(sender, {
                                        text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${dlUrl}${DEFAULT_FOOTER}`
                                    }, { quoted: dlMek });
                                }

                                await fs.remove(localFile).catch(() => {});

                            } catch (downloadErr) {
                                console.error('[Animexin] download error:', downloadErr.message);
                                await socket.sendMessage(sender, {
                                    text: `❌ *Download Error:* _${downloadErr.message}_\n\n🔗 *Direct Link:*\n${dlUrl}${DEFAULT_FOOTER}`
                                }, { quoted: dlMek });
                                try { await fs.remove(localFile); } catch {}
                            }
                        };

                        axDownloadListener = handleDownload;
                        socket.ev.on('messages.upsert', axDownloadListener);
                    }

                    // ═══ EPISODES LIST ═══
                    if (episodes.length > 0) {
                        let epListText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗘𝗣𝗜𝗦𝗢𝗗𝗘𝗦 ❫*\n\n📺 *Total Episodes:* ${episodes.length}\n\n`;
                        episodes.slice(0, 10).forEach((ep, idx) => {
                            epListText += `*${idx + 1}.* Ep ${ep.episode}: _${ep.title || 'Episode'}_\n`;
                        });
                        if (episodes.length > 10) epListText += `\n_...and ${episodes.length - 10} more episodes!_\n`;
                        epListText += `\n📌 _Reply with episode number to download._${DEFAULT_FOOTER}`;

                        const epMsg = await socket.sendMessage(sender, { text: epListText }, { quoted: infoMsg });
                        const epMsgID = epMsg.key.id;

                        // ═══ EPISODE SELECT HANDLER ═══
                        const handleEpisode = async ({ messages: epMsgs }) => {
                            const epMek = epMsgs?.[0];
                            if (!epMek?.message || epMek.key.remoteJid !== sender) return;

                            const epChoiceText = (epMek.message.conversation || epMek.message.extendedTextMessage?.text || '').trim();
                            if (epMek.message.extendedTextMessage?.contextInfo?.stanzaId !== epMsgID) return;

                            const epIdx = parseInt(epChoiceText) - 1;
                            if (isNaN(epIdx) || epIdx < 0 || epIdx >= episodes.length) {
                                return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${episodes.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: epMek });
                            }

                            clearAllAxListeners();
                            const selectedEp = episodes[epIdx];

                            await socket.sendMessage(sender, { react: { text: '📥', key: epMek.key } });
                            await socket.sendMessage(sender, {
                                text: `⏳ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗘𝗣𝗜𝗦𝗢𝗗𝗘*\n\n📺 *Ep ${selectedEp.episode}:* _${selectedEp.title}_\n\n_Fetching download link..._`
                            }, { quoted: epMek });

                            try {
                                const epRes = await axios.get(`${API_BASE}/api/v1/anime/animexin/episode?url=${encodeURIComponent(selectedEp.url)}&api_key=${API_KEY}`, {
                                    timeout: 120000
                                });
                                const epInfo = epRes.data.data || {};
                                const epDls = epInfo.downloads || [];

                                if (epDls.length === 0) {
                                    throw new Error('Episode download links හමු නොවීය.');
                                }

                                const epFileUrl = epDls[0].url || epDls[0].direct_download;

                                // Server download
                                await fs.ensureDir(TEMP_DIR);
                                const safeEpName = `Ep${selectedEp.episode}_${(selectedEp.title || '').replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 30)}`;
                                const localEpFile = path.join(TEMP_DIR, `${safeEpName}_${Date.now()}.mp4`);

                                await downloadToServer(epFileUrl, localEpFile);
                                const epStats = await fs.stat(localEpFile);
                                const epSizeMB = epStats.size / 1024 / 1024;

                                if (epSizeMB < 1) {
                                    await fs.remove(localEpFile).catch(() => {});
                                    throw new Error('File too small — error page');
                                }

                                await socket.sendMessage(sender, {
                                    document: { url: localEpFile },
                                    mimetype: 'video/mp4',
                                    fileName: `${safeEpName}.mp4`,
                                    caption: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗘𝗣𝗜𝗦𝗢𝗗𝗘*\n\n📺 *Series:* ${animeInfo.title || selectedItem.title}\n📌 *Episode:* ${selectedEp.episode}\n🎬 *Title:* ${selectedEp.title}\n📦 *Size:* ${epSizeMB.toFixed(1)} MB\n> 🐉 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🐉`
                                }, { quoted: epMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: epMek.key } });
                                await fs.remove(localEpFile).catch(() => {});

                            } catch (epErr) {
                                await socket.sendMessage(sender, {
                                    text: `❌ *Episode Error:* _${epErr.message}_\n\n🔗 *Episode URL:*\n${selectedEp.url}${DEFAULT_FOOTER}`
                                }, { quoted: epMek });
                            }
                        };

                        axEpisodeListener = handleEpisode;
                        socket.ev.on('messages.upsert', axEpisodeListener);
                    }

                } catch (detailsErr) {
                    clearAllAxListeners();
                    await socket.sendMessage(sender, {
                        text: `❌ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗘𝗥𝗥𝗢𝗥:* ${detailsErr.message}${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                }
            }
        };

        axSelectionListener = handleSelection;
        socket.ev.on('messages.upsert', axSelectionListener);

    } catch (err) {
        clearAllAxListeners();
        await socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗘𝗥𝗥𝗢𝗥 ❫*\n\n❌ *Search Error:* ${err.message}${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }
    break;
}
// ==========================================
// CINESUBZ / CINETV - SHAGGY XMD
// ==========================================
case 'cin':
case 'cinz':
case 'cmovie': {
    const DEFAULT_FOOTER = `\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;
    const TEMP_DIR = './tmp_cinesubz';

    if (!args.length) {
        return socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 ❫*\n\n⚠️ *Invalid Usage!*\n\n🎬 *Example:*\n• .cinesubz avatar\n• .cinetv game of thrones\n• .cmovie spider man\n\n📝 _Please provide Movie or TV Series name!_${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }

    const cinesubQuery = args.join(' ').trim();
    const API_BASE = "https://api.chamindu.site";
    const API_KEY = "chama_api_11230a80e5eed3c1b80bfcc5d1773ec9";
    const DEFAULT_IMAGE = "https://api.chamindu.site/logo.png";

    let csSelectionListener = null;
    let csDownloadListener = null;
    let csMasterTimeout = null;

    const clearAllCsListeners = () => {
        if (csSelectionListener) { socket.ev.off('messages.upsert', csSelectionListener); csSelectionListener = null; }
        if (csDownloadListener)  { socket.ev.off('messages.upsert', csDownloadListener);  csDownloadListener  = null; }
        if (csMasterTimeout)     { clearTimeout(csMasterTimeout); csMasterTimeout = null; }
    };

    const parseSizeMB = (s) => {
        if (!s) return 0;
        const m = s.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    // ⭐ Server download
    const downloadToServer = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 5,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://cinesubz.co/',
                'Accept': '*/*'
            }
        });
        res.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });
    };

    await socket.sendMessage(sender, {
        text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗦𝗘𝗔𝗥𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🔍 *Searching CineSubz for:* _${cinesubQuery}_\n⚡ _Please wait..._`
    }, { quoted: msg });

    try {
        // ═══ STEP 1 : SEARCH ═══
        const searchResponse = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/search?q=${encodeURIComponent(cinesubQuery)}&api_key=${API_KEY}`, {
            timeout: 120000
        });
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            return socket.sendMessage(sender, {
                text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 ❫*\n\n😞 *No Results Found!*\n🎬 *Query:* _${cinesubQuery}_${DEFAULT_FOOTER}`
            }, { quoted: msg });
        }

        const cinesubResults = searchData.data.slice(0, 25);
        let listText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗖𝗜𝗡𝗘𝗦𝗨𝗕𝗭 ❫*\n\n🎯 *Query:* _${cinesubQuery}_\n📊 *Results:* _${cinesubResults.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;

        cinesubResults.forEach((item, index) => {
            const typeIcon = item.type === 'tvshows' ? '📺' : '🎥';
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            listText += `*${num}* ➜ ${typeIcon} _${item.title.substring(0, 30)}_\n`;
        });

        listText += `${DEFAULT_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, { text: listText }, { quoted: msg });
        const messageID = sentMsg.key.id;

        csMasterTimeout = setTimeout(clearAllCsListeners, 180000);

        // ═══ STEP 2 : USER PICKS ═══
        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= cinesubResults.length) {
                    return socket.sendMessage(sender, {
                        text: `⚠️ *Invalid choice! Range: 01 - ${cinesubResults.length}*${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                }

                if (csSelectionListener) { socket.ev.off('messages.upsert', csSelectionListener); csSelectionListener = null; }

                const selectedItem = cinesubResults[choice];
                const isTvShow = selectedItem.type === 'tvshows';

                // ═══ TV SERIES FLOW ═══
                if (isTvShow) {
                    await socket.sendMessage(sender, {
                        text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 ❫*\n\n📺 *Fetching TV Series...*\n⚡ _Please wait..._`
                    }, { quoted: replyMek });

                    try {
                        const tvShowResponse = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/tv/info?q=${encodeURIComponent(selectedItem.link)}&api_key=${API_KEY}`, {
                            timeout: 120000
                        });
                        const tvShowData = tvShowResponse.data;

                        if (!tvShowData.status || !tvShowData.data) {
                            throw new Error('Failed to fetch TV show details');
                        }

                        const tvInfo = tvShowData.data;
                        const episodes = tvInfo.episodes || [];

                        let tvDetailsText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗧𝗩 𝗦𝗘𝗥𝗜𝗘𝗦 ❫*\n\n`;
                        tvDetailsText += `📺 *${tvInfo.title}*\n`;
                        tvDetailsText += `⭐ *IMDb:* ${tvInfo.rating || 'N/A'}\n`;
                        tvDetailsText += `📅 *Year:* ${tvInfo.year || 'N/A'}\n`;
                        if (tvInfo.duration) tvDetailsText += `⏳ *Runtime:* ${tvInfo.duration}\n`;
                        if (tvInfo.country) tvDetailsText += `🌍 *Country:* ${tvInfo.country}\n`;
                        if (tvInfo.genres?.length) tvDetailsText += `🎭 *Genres:* ${tvInfo.genres.join(', ')}\n`;
                        if (tvInfo.directors) tvDetailsText += `🎬 *Director:* ${tvInfo.directors}\n`;
                        if (tvInfo.stars) tvDetailsText += `⭐ *Stars:* ${tvInfo.stars}\n`;
                        tvDetailsText += `🎬 *Episodes:* ${episodes.length}\n`;
                        if (tvInfo.story) tvDetailsText += `\n📝 *Story:* _${tvInfo.story.substring(0, 250)}..._\n`;
                        tvDetailsText += DEFAULT_FOOTER;

                        await socket.sendMessage(sender, {
                            image: { url: tvInfo.image || selectedItem.image || DEFAULT_IMAGE },
                            caption: tvDetailsText
                        }, { quoted: replyMek });

                        if (episodes.length === 0) {
                            return socket.sendMessage(sender, { text: `⚠️ *No episodes found.*${DEFAULT_FOOTER}` }, { quoted: replyMek });
                        }

                        // ═══ EPISODES AUTO DOWNLOAD ═══
                        await socket.sendMessage(sender, {
                            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ❫*\n\n📺 *Series:* _${tvInfo.title}_\n🎬 *Episodes:* _${episodes.length}_\n⚡ _Starting download process..._${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });

                        let successCount = 0;
                        let failCount = 0;

                        for (let i = 0; i < episodes.length; i++) {
                            const episode = episodes[i];
                            try {
                                await socket.sendMessage(sender, {
                                    text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ❫*\n\n🎥 *Episode:* _${episode.episode_name || episode.name || 'Episode ' + (i + 1)}_\n📊 *Progress:* _${i + 1}/${episodes.length}_`
                                }, { quoted: replyMek });

                                const epUrl = episode.episode_url || episode.url || episode.link;
                                const epDlRes = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/tv/dl?q=${encodeURIComponent(epUrl)}&api_key=${API_KEY}`, {
                                    timeout: 120000
                                });
                                const epDlData = epDlRes.data;

                                if (epDlData.status && epDlData.data && epDlData.data.length > 0) {
                                    const nonTgLinks = epDlData.data.filter(link =>
                                        link.link && !link.link.includes('t.me') && !link.link.includes('telegram')
                                    );
                                    const finalLinkObj = nonTgLinks[0] || epDlData.data[0];
                                    const epFileUrl = finalLinkObj.link;

                                    // ⭐ Server download
                                    await fs.ensureDir(TEMP_DIR);
                                    const safeEpName = (episode.episode_name || episode.name || `Ep${i + 1}`).replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 60);
                                    const localEpFile = path.join(TEMP_DIR, `${safeEpName}_${Date.now()}.mp4`);

                                    try {
                                        await downloadToServer(epFileUrl, localEpFile);
                                        const epStats = await fs.stat(localEpFile);
                                        const epSizeMB = epStats.size / 1024 / 1024;

                                        if (epSizeMB < 1) {
                                            await fs.remove(localEpFile).catch(() => {});
                                            throw new Error('Episode file too small (error page)');
                                        }

                                        await socket.sendMessage(sender, {
                                            document: { url: localEpFile },
                                            mimetype: 'video/mp4',
                                            fileName: `${tvInfo.title} - ${episode.episode_name || 'Episode ' + (i + 1)}.mp4`,
                                            caption: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗧𝗩 𝗘𝗣𝗜𝗦𝗢𝗗𝗘*\n\n📺 *Series:* ${tvInfo.title}\n📌 *Episode:* ${episode.episode_name || 'Episode ' + (i + 1)}\n📦 *Size:* ${epSizeMB.toFixed(1)} MB\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
                                        }, { quoted: replyMek });

                                        await fs.remove(localEpFile).catch(() => {});
                                        successCount++;

                                    } catch (epDownloadErr) {
                                        console.error(`Episode ${i + 1} download error:`, epDownloadErr.message);
                                        await socket.sendMessage(sender, {
                                            text: `⚠️ *Episode ${i + 1} Fail:* ${epDownloadErr.message}\n🔗 Link: ${epFileUrl}`
                                        }, { quoted: replyMek });
                                        failCount++;
                                        try { await fs.remove(localEpFile); } catch {}
                                    }
                                } else {
                                    failCount++;
                                }

                                await new Promise(resolve => setTimeout(resolve, 2500));

                            } catch (epError) {
                                console.error(`Error downloading episode:`, epError);
                                failCount++;
                            }
                        }

                        await socket.sendMessage(sender, {
                            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗦𝗨𝗠𝗠𝗔𝗥𝗬 ❫*\n\n🎉 *Download Complete!*\n\n🎬 *Series:* _${tvInfo.title}_\n✅ *Success:* _${successCount} Episodes_\n❌ *Failed:* _${failCount} Episodes_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });

                        return;
                    } catch (tvShowError) {
                        console.error('TV Show error:', tvShowError);
                        return socket.sendMessage(sender, {
                            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗘𝗥𝗥𝗢𝗥 ❫*\n\n❌ *TV Details Error!*\n🚫 _${tvShowError.message}_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                    }
                }

                // ═══ MOVIE FLOW ═══
                await socket.sendMessage(sender, {
                    text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🎬 *Fetching Movie details...*\n⚡ _Please wait..._`
                }, { quoted: replyMek });

                try {
                    const detailsResponse = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/infodl?q=${encodeURIComponent(selectedItem.link)}&api_key=${API_KEY}`, {
                        timeout: 120000
                    });
                    const detailsData = detailsResponse.data;

                    if (!detailsData.status || !detailsData.data) {
                        throw new Error('Failed to fetch details');
                    }

                    const movieInfo = detailsData.data;
                    const validDownloads = movieInfo.downloads || [];

                    if (validDownloads.length === 0) {
                        return socket.sendMessage(sender, {
                            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 ❫*\n\n⚠️ *No Downloads Found!*\n😞 _No downloads available for this movie._${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                    }

                    let movieDetailsText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗠𝗢𝗩𝗜𝗘 ❫*\n\n`;
                    movieDetailsText += `🎬 *${movieInfo.title}*\n`;
                    movieDetailsText += `⭐ *IMDb:* ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n`;
                    movieDetailsText += `📅 *Year:* ${movieInfo.year || 'N/A'}\n`;
                    if (movieInfo.duration) movieDetailsText += `⏳ *Duration:* ${movieInfo.duration}\n`;
                    if (movieInfo.country) movieDetailsText += `🌍 *Country:* ${movieInfo.country}\n`;
                    if (movieInfo.genres?.length) movieDetailsText += `🎭 *Genres:* ${movieInfo.genres.join(', ')}\n`;
                    if (movieInfo.language) movieDetailsText += `🏷️ *Language:* ${movieInfo.language}\n`;
                    if (movieInfo.directors) movieDetailsText += `🎬 *Director:* ${movieInfo.directors}\n`;
                    if (movieInfo.stars) movieDetailsText += `⭐ *Stars:* ${movieInfo.stars}\n`;
                    if (movieInfo.story) movieDetailsText += `\n📝 *Story:* _${movieInfo.story.substring(0, 250)}..._\n`;
                    movieDetailsText += DEFAULT_FOOTER;

                    const posterUrl = movieInfo.image || selectedItem.image || DEFAULT_IMAGE;
                    await socket.sendMessage(sender, {
                        image: { url: posterUrl },
                        caption: movieDetailsText
                    }, { quoted: replyMek });

                    // Download options
                    let dlText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗦 ❫*\n\n📥 *Select Quality:*\n\n`;
                    validDownloads.slice(0, 20).forEach((dl, i) => {
                        const num = (i + 1) < 10 ? `0${i + 1}` : `${i + 1}`;
                        const sizeMB = parseSizeMB(dl.size);
                        const note = sizeMB > 2000 ? ' ⚠️' : ' ✓';
                        const qualityIcon = (dl.quality || '').includes('1080') ? '🔥' : (dl.quality || '').includes('720') ? '💎' : '📱';
                        dlText += `*${num}* ➜ ${qualityIcon} _${dl.quality || 'HD'}_ (${dl.size || 'N/A'})${note}\n`;
                    });
                    dlText += `\n📌 _Reply with number to send file._${DEFAULT_FOOTER}`;

                    const dlSentMsg = await socket.sendMessage(sender, { text: dlText }, { quoted: replyMek });
                    const dlMessageID = dlSentMsg.key.id;

                    // ═══ DOWNLOAD HANDLER ═══
                    const handleDownload = async ({ messages: dlReplyMessages }) => {
                        const dlReplyMek = dlReplyMessages[0];
                        if (!dlReplyMek?.message || dlReplyMek.key.remoteJid !== sender) return;

                        const dlChoiceText = dlReplyMek.message.conversation || dlReplyMek.message.extendedTextMessage?.text;
                        if (dlReplyMek.message.extendedTextMessage?.contextInfo?.stanzaId !== dlMessageID) return;

                        const dlChoice = parseInt(dlChoiceText) - 1;
                        if (isNaN(dlChoice) || dlChoice < 0 || dlChoice >= validDownloads.length) {
                            return socket.sendMessage(sender, { text: `⚠️ *Invalid quality number!*` }, { quoted: dlReplyMek });
                        }

                        clearAllCsListeners();
                        const selectedDownload = validDownloads[dlChoice];
                        const fileUrl = selectedDownload.link;
                        const sizeMB = parseSizeMB(selectedDownload.size);

                        await socket.sendMessage(sender, { react: { text: '📥', key: dlReplyMek.key } });

                        // ⚠️ 2GB limit
                        if (sizeMB > 2000) {
                            return socket.sendMessage(sender, {
                                text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${movieInfo.title}*\n📌 *${selectedDownload.quality}*\n📦 *${selectedDownload.size}*\n\n🔗 *Direct Link:*\n${fileUrl}\n\n_IDM එකෙන් download කරන්න._${DEFAULT_FOOTER}`
                            }, { quoted: dlReplyMek });
                        }

                        await socket.sendMessage(sender, {
                            text: `⏳ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚*\n\n📌 *${selectedDownload.quality}*\n📦 *Size:* ${selectedDownload.size || 'N/A'}\n\n_කරුණාකර රැඳී සිටින්න..._`
                        }, { quoted: dlReplyMek });

                        // ⭐ Server download
                        await fs.ensureDir(TEMP_DIR);
                        const safeName = movieInfo.title.replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 50);
                        const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.mp4`);

                        try {
                            await downloadToServer(fileUrl, localFile);
                            const stats = await fs.stat(localFile);
                            const realSizeMB = stats.size / 1024 / 1024;

                            if (realSizeMB < 1) {
                                await fs.remove(localFile).catch(() => {});
                                throw new Error('Download failed — file too small (error page detected)');
                            }

                            await socket.sendMessage(sender, {
                                text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                            }, { quoted: dlReplyMek });

                            try {
                                await socket.sendMessage(sender, {
                                    document: { url: localFile },
                                    mimetype: 'video/mp4',
                                    fileName: `${safeName} - ${selectedDownload.quality || 'HD'}.mp4`,
                                    caption: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗖𝗜𝗡𝗘𝗦𝗨𝗕𝗭*\n\n🎬 *Title:* ${movieInfo.title}\n⭐ *IMDb:* ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n📅 *Year:* ${movieInfo.year || 'N/A'}\n📌 *Quality:* ${selectedDownload.quality || 'HD'}\n📦 *Size:* ${selectedDownload.size || 'N/A'}\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
                                }, { quoted: dlReplyMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: dlReplyMek.key } });

                            } catch (sendErr) {
                                await socket.sendMessage(sender, {
                                    text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${fileUrl}${DEFAULT_FOOTER}`
                                }, { quoted: dlReplyMek });
                            }

                            await fs.remove(localFile).catch(() => {});

                        } catch (downloadErr) {
                            console.error('[CineSubz] download error:', downloadErr.message);
                            await socket.sendMessage(sender, {
                                text: `❌ *Download Error:* _${downloadErr.message}_\n\n🔗 *Direct Link:*\n${fileUrl}${DEFAULT_FOOTER}`
                            }, { quoted: dlReplyMek });
                            try { await fs.remove(localFile); } catch {}
                        }
                    };

                    csDownloadListener = handleDownload;
                    socket.ev.on('messages.upsert', csDownloadListener);

                } catch (detailsError) {
                    clearAllCsListeners();
                    await socket.sendMessage(sender, {
                        text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗘𝗥𝗥𝗢𝗥 ❫*\n\n❌ *Movie Details Error!*\n🚫 _${detailsError.message}_${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                }
            }
        };

        csSelectionListener = handleSelection;
        socket.ev.on('messages.upsert', csSelectionListener);

    } catch (error) {
        clearAllCsListeners();
        await socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗘𝗥𝗥𝗢𝗥 ❫*\n\n❌ *System Error!*\n🚫 _${error.message || 'Unknown error'}_${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }
    break;
}
// ==========================================
// WATCHWRESTLING - Fixed (Server Download)
// ==========================================
case 'wrestling':
case 'watchwrestling': {
    const chatJid = msg.key.remoteJid;
    const senderJid = msg.key.participant || msg.key.remoteJid;
    const isGroup = chatJid.endsWith('@g.us');
    const TEMP_DIR = './tmp_wrestling';

    if (!args.length) {
        const errorCaption = typeof formatMessage === 'function' 
            ? formatMessage('❌ ERROR', '*කරුණාකර සෙවිය යුතු Wrestling Show එකේ නම ලබාදෙන්න! උදා: .wrestling Raw*', `${sessionConfig?.BOT_FOOTER || config?.BOT_FOOTER || ''}`)
            : `❌ *ERROR*\n\n*කරුණාකර සෙවිය යුතු Wrestling Show එකේ නම ලබාදෙන්න! උදා: .wrestling Raw*\n\n> ${sessionConfig?.BOT_FOOTER || config?.BOT_FOOTER || ''}`;
        
        try {
            await socket.sendMessage(chatJid, {
                image: { url: sessionConfig?.BOT_IMAGE || config?.BOT_IMAGE || 'https://api.chamindu.site/logo.png' },
                caption: errorCaption
            }, { quoted: msg });
        } catch {
            await socket.sendMessage(chatJid, { text: errorCaption }, { quoted: msg });
        }
        break;
    }

    const wrestlingQuery = args.join(' ').trim();
    const API_BASE = 'https://api.chamindu.site/api/v1/wrestling/watchwrestling';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    let wrestlingSelectionListener = null;
    let wrestlingDownloadListener = null;
    let wrestlingMasterTimeout = null;

    const clearAllWrestlingListeners = () => {
        if (wrestlingSelectionListener) { socket.ev.off('messages.upsert', wrestlingSelectionListener); wrestlingSelectionListener = null; }
        if (wrestlingDownloadListener)  { socket.ev.off('messages.upsert', wrestlingDownloadListener);  wrestlingDownloadListener  = null; }
        if (wrestlingMasterTimeout)     { clearTimeout(wrestlingMasterTimeout); wrestlingMasterTimeout = null; }
    };

    const parseSizeMB = (s) => {
        if (!s) return 0;
        const m = s.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    // ⭐ Download to server
    const downloadToServer = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 5,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://watchwrestling.ws/',
                'Accept': '*/*'
            }
        });
        res.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });
    };

    try {
        await socket.sendMessage(chatJid, { text: '🔍 *Searching shows on WatchWrestling...*' }, { quoted: msg });

        // ═══ STEP 1 : SEARCH ═══
        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: wrestlingQuery, api_key: API_KEY },
            timeout: 120000
        });

        const searchData = searchRes.data;
        const results = searchData?.data || [];
        if (!searchData?.status || results.length === 0) {
            await socket.sendMessage(chatJid, {
                text: `❌ *NO RESULTS*\n\n*කිසිදු Wrestling Show එකක් හමු නොවීය! (Query: ${wrestlingQuery})*\n\n> ${sessionConfig?.BOT_FOOTER || config?.BOT_FOOTER || ''}`
            }, { quoted: msg });
            break;
        }

        const showList = results.slice(0, 10);
        let listText = `🤼 *𝗪𝗔𝗧𝗖𝗛𝗪??𝗘𝗦𝗧𝗟𝗜𝗡𝗚 𝗦𝗘𝗔𝗥𝗖𝗛 : _${wrestlingQuery}_*\n╭──────●➤\n*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ (1 - ${showList.length})*\n╰──────────●➤\n╭──────●➤\n`;

        showList.forEach((item, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${item.title}*\n    ↳ (📅 ${item.date || 'N/A'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig?.BOT_FOOTER || config?.BOT_FOOTER || ''}`;

        let searchMsg;
        try {
            const thumb = showList[0]?.image || sessionConfig?.BOT_IMAGE || config?.BOT_IMAGE;
            searchMsg = await socket.sendMessage(chatJid, { image: { url: thumb }, caption: listText }, { quoted: msg });
        } catch {
            searchMsg = await socket.sendMessage(chatJid, { text: listText }, { quoted: msg });
        }

        const searchMsgID = searchMsg.key.id;
        wrestlingMasterTimeout = setTimeout(clearAllWrestlingListeners, 180000);

        // ═══ STEP 2 : USER PICKS A SHOW ═══
        const handleShowSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== chatJid) return;

            const replier = replyMek.key.participant || replyMek.key.remoteJid;
            if (replier !== senderJid) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;
            const isDirectNum = !isNaN(parseInt(text)) && parseInt(text) >= 1 && parseInt(text) <= showList.length;

            if (isReply || (!isGroup && isDirectNum)) {
                const choice = parseInt(text) - 1;
                if (isNaN(choice) || choice < 0 || choice >= showList.length) {
                    return socket.sendMessage(chatJid, { text: `❌ කරුණාකර 1 - ${showList.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: replyMek });
                }

                if (wrestlingSelectionListener) { socket.ev.off('messages.upsert', wrestlingSelectionListener); wrestlingSelectionListener = null; }

                const chosenShow = showList[choice];
                await socket.sendMessage(chatJid, { text: '⏳ *Fetching direct download sources...*' }, { quoted: replyMek });

                try {
                    // ═══ STEP 3 : INFO ═══
                    const infoRes = await axios.get(`${API_BASE}/info`, {
                        params: { q: chosenShow.url, api_key: API_KEY },
                        timeout: 120000
                    });

                    const showData = infoRes.data?.data;
                    const downloads = showData?.downloads || [];

                    if (!showData || downloads.length === 0) throw new Error('සෘජු බාගත කිරීමේ Direct Downloads හමු නොවීය.');

                    let infoText = `🔥 *${showData.title || chosenShow.title}*\n\n`;
                    if (showData.date) infoText += `📅 *Date:* ${showData.date}\n`;
                    if (showData.description) infoText += `📝 *Info:* _${showData.description.substring(0, 120)}..._\n\n`;

                    infoText += `*⚡ Select Direct Download Quality:*\n`;
                    downloads.forEach((dl, i) => {
                        const qual = dl.quality || 'HD';
                        const hoster = dl.hoster || 'Direct';
                        const sizeMB = parseSizeMB(dl.size);
                        const note = sizeMB > 2000 ? ' ⚠️' : ' ✓';
                        infoText += `*${i + 1}.* 📥 *[${qual}]* ${hoster}${note}\n`;
                    });
                    infoText += `\n👉 *බාගත කිරීමට අදාළ Quality අංකය Reply කරන්න.*\n_✓ = Document • ⚠️ = 2GB+_`;

                    let infoMsg;
                    try {
                        const imgUrl = showData.image || chosenShow.image;
                        infoMsg = await socket.sendMessage(chatJid, { image: { url: imgUrl }, caption: infoText }, { quoted: replyMek });
                    } catch {
                        infoMsg = await socket.sendMessage(chatJid, { text: infoText }, { quoted: replyMek });
                    }

                    const infoMsgID = infoMsg.key.id;

                    // ═══ STEP 4 : USER PICKS DOWNLOAD ═══
                    const handleDownloadSelection = async ({ messages: dlMessages }) => {
                        const dlMek = dlMessages?.[0];
                        if (!dlMek?.message || dlMek.key.remoteJid !== chatJid) return;

                        const dlReplier = dlMek.key.participant || dlMek.key.remoteJid;
                        if (dlReplier !== senderJid) return;

                        const dlChoiceText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                        const isDlReply = dlMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;
                        const isDirectDlNum = !isNaN(parseInt(dlChoiceText)) && parseInt(dlChoiceText) >= 1 && parseInt(dlChoiceText) <= downloads.length;

                        if (isDlReply || (!isGroup && isDirectDlNum)) {
                            const dlIdx = parseInt(dlChoiceText) - 1;
                            if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= downloads.length) {
                                return socket.sendMessage(chatJid, { text: `❌ කරුණාකර 1 - ${downloads.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: dlMek });
                            }

                            clearAllWrestlingListeners();
                            const selectedSource = downloads[dlIdx];
                            const sourceUrl = selectedSource.direct_link || selectedSource.url || selectedSource.link;
                            const sizeMB = parseSizeMB(selectedSource.size);

                            await socket.sendMessage(chatJid, { react: { text: '📥', key: dlMek.key } });

                            // ⚠️ 2GB limit
                            if (sizeMB > 2000) {
                                return socket.sendMessage(chatJid, {
                                    text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🤼 *${showData.title || chosenShow.title}*\n📌 *${selectedSource.quality || 'HD'}*\n📦 *${selectedSource.size}*\n\n🔗 *Direct Link:*\n${sourceUrl}\n\n_IDM එකෙන් download කරන්න._\n> ${sessionConfig?.BOT_FOOTER || config?.BOT_FOOTER || ''}`
                                }, { quoted: dlMek });
                            }

                            await socket.sendMessage(chatJid, {
                                text: `⏳ *Downloading to Server...*\n📌 *${selectedSource.quality || 'HD'}*\n📦 *Size:* ${selectedSource.size || 'N/A'}\n\n_කරුණාකර රැඳී සිටින්න..._`
                            }, { quoted: dlMek });

                            // ⭐ Server download
                            await fs.ensureDir(TEMP_DIR);
                            const safeName = (showData.title || chosenShow.title).replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 50);
                            const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.mp4`);

                            try {
                                await downloadToServer(sourceUrl, localFile);

                                const stats = await fs.stat(localFile);
                                const realSizeMB = stats.size / 1024 / 1024;

                                // ⚠️ Error page check
                                if (realSizeMB < 1) {
                                    await fs.remove(localFile).catch(() => {});
                                    throw new Error('Download failed — file too small (error page detected)');
                                }

                                await socket.sendMessage(chatJid, {
                                    text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                                }, { quoted: dlMek });

                                // ⭐ Send as document
                                const fileName = `${safeName} - ${selectedSource.quality || 'HD'}.mp4`;

                                try {
                                    await socket.sendMessage(chatJid, {
                                        document: { url: localFile },
                                        mimetype: 'video/mp4',
                                        fileName: fileName,
                                        caption: `✅ *WRESTLING SHOW*\n\n🤼 *Show:* ${showData.title || chosenShow.title}\n📅 *Date:* ${showData.date || 'N/A'}\n📌 *Quality:* ${selectedSource.quality || 'HD'}\n📦 *Size:* ${selectedSource.size || 'N/A'}\n> ${sessionConfig?.BOT_FOOTER || config?.BOT_FOOTER || ''}`
                                    }, { quoted: dlMek });

                                    await socket.sendMessage(chatJid, { react: { text: '✅', key: dlMek.key } });

                                } catch (sendErr) {
                                    await socket.sendMessage(chatJid, {
                                        text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${sourceUrl}\n\n_IDM එකෙන් download කරන්න._`
                                    }, { quoted: dlMek });
                                }

                                // Cleanup
                                await fs.remove(localFile).catch(() => {});

                            } catch (downloadErr) {
                                console.error('[Wrestling] download error:', downloadErr.message);
                                await socket.sendMessage(chatJid, {
                                    text: `❌ *Download Error:* _${downloadErr.message}_\n\n🔗 *Direct Link:*\n${sourceUrl}\n\n💡 _IDM එකෙන් download කරන්න._`
                                }, { quoted: dlMek });

                                try { await fs.remove(localFile); } catch {}
                            }
                        }
                    };

                    wrestlingDownloadListener = handleDownloadSelection;
                    socket.ev.on('messages.upsert', wrestlingDownloadListener);

                } catch (infoErr) {
                    clearAllWrestlingListeners();
                    await socket.sendMessage(chatJid, { text: `❌ WatchWrestling Info Error: ${infoErr.message}` }, { quoted: replyMek });
                }
            }
        };

        wrestlingSelectionListener = handleShowSelection;
        socket.ev.on('messages.upsert', wrestlingSelectionListener);

    } catch (err) {
        clearAllWrestlingListeners();
        await socket.sendMessage(chatJid, { text: `❌ Error: ${err.message}` }, { quoted: msg });
    }
    break;
}
// ==========================================
// SUBZLK - Fixed (Server Download + Auto Send)
// ==========================================
case 'subzlk':
case 'subz': {
    const chatJid = msg.key.remoteJid;
    const senderJid = msg.key.participant || msg.key.remoteJid;
    const isGroup = chatJid.endsWith('@g.us');
    const TEMP_DIR = './tmp_subzlk';

    if (!args.length) {
        await socket.sendMessage(chatJid, {
            text: `❌ *ERROR*\n\n*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .subzlk Vaazha*\n\n> ${sessionConfig?.BOT_FOOTER || config?.BOT_FOOTER || ''}`
        }, { quoted: msg });
        break;
    }

    const movieQuery = args.join(' ').trim();
    const API_BASE = 'https://api.chamindu.site/api/v1/movies/subzlk';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    let subzSelectionListener = null;
    let subzDownloadListener = null;
    let subzMasterTimeout = null;

    const clearAllSubzListeners = () => {
        if (subzSelectionListener) { socket.ev.off('messages.upsert', subzSelectionListener); subzSelectionListener = null; }
        if (subzDownloadListener)  { socket.ev.off('messages.upsert', subzDownloadListener);  subzDownloadListener  = null; }
        if (subzMasterTimeout)     { clearTimeout(subzMasterTimeout); subzMasterTimeout = null; }
    };

    const cleanSubzTitle = (t = '') =>
        t.replace(/\s*\|\s*සිංහල උපසිරැසි.*$/i, '')
         .replace(/\s*Sinhala Subtitles.*$/i, '')
         .replace(/\s*\|.*$/i, '')
         .trim();

    const parseSizeMB = (s) => {
        if (!s) return 0;
        const m = s.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    // ⭐ Download to server
    const downloadToServer = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 5,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://subzlk.com/',
                'Accept': '*/*'
            }
        });
        res.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });
    };

    try {
        await socket.sendMessage(chatJid, { text: '🔍 *Searching movies on SubzLK...*' }, { quoted: msg });

        // ═══ STEP 1 : SEARCH ═══
        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: movieQuery, api_key: API_KEY },
            timeout: 120000
        });

        const searchData = searchRes.data;
        const results = searchData?.data || [];
        if (!searchData?.status || results.length === 0) {
            await socket.sendMessage(chatJid, {
                text: `❌ *NO RESULTS*\n\n*කිසිදු චිත්‍රපටයක් හමු නොවීය! (Query: ${movieQuery})*\n\n> ${sessionConfig?.BOT_FOOTER || config?.BOT_FOOTER || ''}`
            }, { quoted: msg });
            break;
        }

        const movieList = results.slice(0, 15);
        let listText = `🎬 *𝗦𝗨𝗕𝗭𝗟𝗞 𝗦𝗘𝗔𝗥𝗖𝗛 : _${movieQuery}_*\n╭──────●➤\n*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ (1 - ${movieList.length})*\n╰──────────●➤\n╭──────●➤\n`;

        movieList.forEach((item, index) => {
            listText += `*🎥 ${index + 1} ┃❭❭ ${cleanSubzTitle(item.title)}*\n    ↳ (${item.year || 'N/A'} | ⭐ ${item.rating || 'N/A'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig?.BOT_FOOTER || config?.BOT_FOOTER || ''}`;

        let searchMsg;
        try {
            const thumb = movieList[0]?.image || sessionConfig?.BOT_IMAGE || config?.BOT_IMAGE;
            searchMsg = await socket.sendMessage(chatJid, { image: { url: thumb }, caption: listText }, { quoted: msg });
        } catch {
            searchMsg = await socket.sendMessage(chatJid, { text: listText }, { quoted: msg });
        }

        const searchMsgID = searchMsg.key.id;
        subzMasterTimeout = setTimeout(clearAllSubzListeners, 120000);

        // ═══ STEP 2 : USER PICKS A MOVIE ═══
        const handleMovieSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== chatJid) return;

            const replier = replyMek.key.participant || replyMek.key.remoteJid;
            if (replier !== senderJid) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;
            const isDirectNum = !isNaN(parseInt(text)) && parseInt(text) >= 1 && parseInt(text) <= movieList.length;

            if (isReply || (!isGroup && isDirectNum)) {
                const choice = parseInt(text) - 1;
                if (isNaN(choice) || choice < 0 || choice >= movieList.length) {
                    return socket.sendMessage(chatJid, { text: `❌ කරුණාකර 1 - ${movieList.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: replyMek });
                }

                if (subzSelectionListener) { socket.ev.off('messages.upsert', subzSelectionListener); subzSelectionListener = null; }

                const chosenMovie = movieList[choice];
                await socket.sendMessage(chatJid, { text: '⏳ *Fetching movie download links...*' }, { quoted: replyMek });

                try {
                    // ═══ STEP 3 : INFO ═══
                    const infoRes = await axios.get(`${API_BASE}/infodl`, {
                        params: { q: chosenMovie.link, api_key: API_KEY },
                        timeout: 120000
                    });

                    const movieData = infoRes.data?.data;
                    const allDownloads = movieData?.downloads || [];
                    if (!movieData || allDownloads.length === 0) throw new Error('බාගත කිරීමේ links හමු නොවීය.');

                    let infoText = `🎬 *${cleanSubzTitle(movieData.title)}*\n\n`;
                    if (movieData.imdb) infoText += `⭐ *IMDb:* ${movieData.imdb}\n`;
                    if (movieData.language) infoText += `🗣️ *Language:* ${movieData.language}\n\n`;
                    infoText += `*Available Direct Download Links:*\n`;
                    allDownloads.forEach((dl, i) => {
                        infoText += `*${i + 1}.* 📥 [${dl.quality || 'HD'}] ${dl.size ? `(${dl.size})` : ''}\n`;
                    });
                    infoText += `\n👉 *බාගත කිරීමට අදාළ අංකය Reply කරන්න.*`;

                    let infoMsg;
                    try {
                        const imgUrl = movieData.image || chosenMovie.image;
                        infoMsg = await socket.sendMessage(chatJid, { image: { url: imgUrl }, caption: infoText }, { quoted: replyMek });
                    } catch {
                        infoMsg = await socket.sendMessage(chatJid, { text: infoText }, { quoted: replyMek });
                    }

                    const infoMsgID = infoMsg.key.id;

                    // ═══ STEP 4 : USER PICKS DOWNLOAD ═══
                    const handleDownloadSelection = async ({ messages: dlMessages }) => {
                        const dlMek = dlMessages?.[0];
                        if (!dlMek?.message || dlMek.key.remoteJid !== chatJid) return;

                        const dlReplier = dlMek.key.participant || dlMek.key.remoteJid;
                        if (dlReplier !== senderJid) return;

                        const dlChoiceText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                        const isDlReply = dlMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;
                        const isDirectDlNum = !isNaN(parseInt(dlChoiceText)) && parseInt(dlChoiceText) >= 1 && parseInt(dlChoiceText) <= allDownloads.length;

                        if (isDlReply || (!isGroup && isDirectDlNum)) {
                            const dlIdx = parseInt(dlChoiceText) - 1;
                            if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= allDownloads.length) {
                                return socket.sendMessage(chatJid, { text: `❌ කරුණාකර 1 - ${allDownloads.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: dlMek });
                            }

                            clearAllSubzListeners();
                            const selectedDl = allDownloads[dlIdx];
                            let rawTarget = selectedDl.direct_link || selectedDl.link;

                            await socket.sendMessage(chatJid, { react: { text: '📥', key: dlMek.key } });

                            // ⚠️ 2GB limit check
                            const sizeMB = parseSizeMB(selectedDl.size);
                            if (sizeMB > 2000) {
                                return socket.sendMessage(chatJid, {
                                    text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${cleanSubzTitle(movieData.title)}*\n📌 *${selectedDl.quality}*\n📦 *${selectedDl.size}*\n\n🔗 *Direct Link:*\n${rawTarget}\n\n_IDM එකෙන් download කරන්න._\n> ${sessionConfig?.BOT_FOOTER || config?.BOT_FOOTER || ''}`
                                }, { quoted: dlMek });
                            }

                            await socket.sendMessage(chatJid, {
                                text: `⏳ *Downloading to Server...*\n📌 *${selectedDl.quality}*\n📦 *Size:* ${selectedDl.size || 'N/A'}\n\n_කරුණාකර රැඳී සිටින්න..._`
                            }, { quoted: dlMek });

                            // ⭐ Try to resolve direct link
                            let downloadUrl = rawTarget;
                            try {
                                const resolveRes = await axios.get(`${API_BASE}/dl`, {
                                    params: { url: rawTarget, api_key: API_KEY },
                                    timeout: 120000
                                });
                                if (resolveRes.data?.direct_link) downloadUrl = resolveRes.data.direct_link;
                                else if (resolveRes.data?.download_link) downloadUrl = resolveRes.data.download_link;
                                else if (resolveRes.data?.url) downloadUrl = resolveRes.data.url;
                            } catch (e) {
                                console.log('[SubzLK] dl resolve failed:', e.message);
                            }

                            // ⭐ Server download
                            await fs.ensureDir(TEMP_DIR);
                            const safeName = cleanSubzTitle(movieData.title).replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 50);
                            const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.mp4`);

                            try {
                                await downloadToServer(downloadUrl, localFile);

                                const stats = await fs.stat(localFile);
                                const realSizeMB = stats.size / 1024 / 1024;

                                // ⚠️ Error page check
                                if (realSizeMB < 1) {
                                    await fs.remove(localFile).catch(() => {});
                                    throw new Error('Download failed — file too small (error page detected)');
                                }

                                await socket.sendMessage(chatJid, {
                                    text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                                }, { quoted: dlMek });

                                // ⭐ Send as document
                                const fileName = `${safeName} - ${selectedDl.quality || 'HD'}.mp4`;

                                try {
                                    await socket.sendMessage(chatJid, {
                                        document: { url: localFile },
                                        mimetype: 'video/mp4',
                                        fileName: fileName,
                                        caption: `✅ *SUBZLK MOVIE*\n\n🎬 *Title:* ${cleanSubzTitle(movieData.title)}\n⭐ *IMDb:* ${movieData.imdb || 'N/A'}\n📌 *Quality:* ${selectedDl.quality || 'HD'}\n📦 *Size:* ${selectedDl.size || 'N/A'}\n> ${sessionConfig?.BOT_FOOTER || config?.BOT_FOOTER || ''}`
                                    }, { quoted: dlMek });

                                    await socket.sendMessage(chatJid, { react: { text: '✅', key: dlMek.key } });

                                } catch (sendErr) {
                                    await socket.sendMessage(chatJid, {
                                        text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${rawTarget}\n\n_IDM එකෙන් download කරන්න._`
                                    }, { quoted: dlMek });
                                }

                                // Cleanup
                                await fs.remove(localFile).catch(() => {});

                            } catch (downloadErr) {
                                console.error('[SubzLK] download error:', downloadErr.message);
                                await socket.sendMessage(chatJid, {
                                    text: `❌ *Download Error:* _${downloadErr.message}_\n\n🔗 *Direct Link:*\n${rawTarget}\n\n💡 _IDM එකෙන් download කරන්න._`
                                }, { quoted: dlMek });

                                try { await fs.remove(localFile); } catch {}
                            }
                        }
                    };

                    subzDownloadListener = handleDownloadSelection;
                    socket.ev.on('messages.upsert', subzDownloadListener);

                } catch (infoErr) {
                    clearAllSubzListeners();
                    await socket.sendMessage(chatJid, { text: `❌ SubzLK Info Error: ${infoErr.message}` }, { quoted: replyMek });
                }
            }
        };

        subzSelectionListener = handleMovieSelection;
        socket.ev.on('messages.upsert', subzSelectionListener);   // ✅ Fixed: handleShowSelection → subzSelectionListener

    } catch (err) {
        clearAllSubzListeners();
        await socket.sendMessage(chatJid, { text: `❌ Error: ${err.message}` }, { quoted: msg });
    }
    break;
}
// ==========================================
// 1TAMILMV - Fixed (Server Download)
// ==========================================
case 'tamilmv':
case 'tamil': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .tamilmv Maharaja*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const movieQuery = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/movie/tamilmv';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';
    const TEMP_DIR = './tmp_tamilmv';

    const TIMEOUT_API = 120000;
    const TIMEOUT_INFO = 120000;

    let tmvSelectionListener = null;
    let tmvDownloadListener = null;
    let tmvMasterTimeout = null;

    const clearAllTmvListeners = () => {
        if (tmvSelectionListener) { socket.ev.off('messages.upsert', tmvSelectionListener); tmvSelectionListener = null; }
        if (tmvDownloadListener)  { socket.ev.off('messages.upsert', tmvDownloadListener);  tmvDownloadListener  = null; }
        if (tmvMasterTimeout)     { clearTimeout(tmvMasterTimeout); tmvMasterTimeout = null; }
    };

    const cleanTmvTitle = (t = '') =>
        t.replace(/\s*\-\s*\[.*$/i, '')
         .replace(/\s*\|.*$/i, '')
         .trim();

    const parseSizeMB = (sizeStr) => {
        if (!sizeStr) return 0;
        const m = sizeStr.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    // ⭐ File download to server
    const downloadToServer = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 5,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://www.1tamilmv.meme/',
                'Accept': '*/*'
            }
        });
        res.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching on 1TamilMV...' }, { quoted: msg });

        // ═══ STEP 1 : SEARCH ═══
        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: movieQuery, api_key: API_KEY },
            timeout: TIMEOUT_API
        });

        const searchData = searchRes.data;
        const results = searchData.results || searchData.data || [];

        if (!searchData.status || results.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage('❌ NO RESULTS', '*කිසිදු චිත්‍රපටයක් හමු නොවීය!*', `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
            }, { quoted: msg });
            break;
        }

        const movieList = results.slice(0, 20);
        let listText = `🎬 *𝟭𝗧𝗔𝗠𝗜𝗟𝗠𝗩 𝗦𝗘𝗔𝗥𝗖𝗛 : _${movieQuery}_*\n╭──────●➤\n*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        movieList.forEach((item, index) => {
            const quality = item.quality || 'HD';
            listText += `*🎥 ${index + 1} ┃❭❭ ${cleanTmvTitle(item.title)}*\n    ↳ (${quality} | 📅 ${item.date || 'N/A'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: movieList[0].image || movieList[0].poster || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;
        tmvMasterTimeout = setTimeout(clearAllTmvListeners, 180000);

        // ═══ STEP 2 : USER PICKS A MOVIE ═══
        const handleMovieSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            if (replyMek.message.extendedTextMessage?.contextInfo?.stanzaId !== searchMsgID) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= movieList.length) {
                return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${movieList.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: replyMek });
            }

            if (tmvSelectionListener) { socket.ev.off('messages.upsert', tmvSelectionListener); tmvSelectionListener = null; }

            const chosenMovie = movieList[choice];
            await socket.sendMessage(sender, { text: '⏳ Fetching download links...' }, { quoted: replyMek });

            try {
                // ═══ STEP 3 : INFO + DL ═══
                const infoRes = await axios.get(`${API_BASE}/infodl`, {
                    params: { url: chosenMovie.link, api_key: API_KEY },
                    timeout: TIMEOUT_INFO
                });

                const movieData = infoRes.data;
                const allDownloads = movieData?.downloads || [];
                if (!movieData.status || allDownloads.length === 0) throw new Error('බාගත කිරීමේ links හමු නොවීය.');

                let infoText = `🎬 *${cleanTmvTitle(movieData.title || chosenMovie.title)}*\n\n`;
                if (movieData.year) infoText += `📅 *Year:* ${movieData.year}\n`;
                if (movieData.quality) infoText += `🎞 *Quality:* ${movieData.quality}\n`;
                infoText += `📊 *Total Releases:* ${allDownloads.length}\n\n`;
                infoText += `*Available Downloads:*\n`;

                allDownloads.forEach((dl, i) => {
                    const sizeMB = parseSizeMB(dl.size);
                    let note = '';
                    if (dl.link?.includes('cyberloom')) note = ' 🔗';
                    else if (sizeMB > 2000) note = ' ⚠️';
                    else note = ' ✓';
                    infoText += `*${i + 1}.* ${dl.quality || 'N/A'} _(${dl.size || 'N/A'})_${note}\n`;
                });
                infoText += `\n👉 *බාගත කිරීමට අදාළ අංකය Reply කරන්න.*\n_✓ = Document • 🔗 = Link only • ⚠️ = 2GB+_`;

                const infoMsg = await socket.sendMessage(sender, {
                    image: { url: movieData.image || movieData.poster || chosenMovie.image || pickBotImage(sessionConfig) },
                    caption: infoText
                }, { quoted: replyMek });

                const infoMsgID = infoMsg.key.id;

                // ═══ STEP 4 : USER PICKS DOWNLOAD ═══
                const handleDownloadSelection = async ({ messages: dlMessages }) => {
                    const dlMek = dlMessages?.[0];
                    if (!dlMek?.message || dlMek.key.remoteJid !== sender) return;

                    const dlChoiceText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                    if (dlMek.message.extendedTextMessage?.contextInfo?.stanzaId !== infoMsgID) return;

                    const dlIdx = parseInt(dlChoiceText) - 1;
                    if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= allDownloads.length) {
                        return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${allDownloads.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: dlMek });
                    }

                    clearAllTmvListeners();
                    const selectedDl = allDownloads[dlIdx];
                    const dlUrl = selectedDl.direct_download_url || selectedDl.url || selectedDl.link;
                    const sizeMB = parseSizeMB(selectedDl.size);
                    const isCyberloom = dlUrl.includes('cyberloom');

                    await socket.sendMessage(sender, { react: { text: '📥', key: dlMek.key } });

                    // 🔗 Cyberloom → link only
                    if (isCyberloom) {
                        return socket.sendMessage(sender, {
                            text: `🔗 *Link Only*\n\n🎬 *${cleanTmvTitle(movieData.title || chosenMovie.title)}*\n📌 *Quality:* ${selectedDl.quality}\n📦 *Size:* ${selectedDl.size}\n\n🔗 *Download Link:*\n${dlUrl}\n\n_මෙය browser එකෙන් හෝ IDM එකෙන් download කරන්න._\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: dlMek });
                    }

                    // ⚠️ 2GB limit
                    if (sizeMB > 2000) {
                        return socket.sendMessage(sender, {
                            text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${cleanTmvTitle(movieData.title || chosenMovie.title)}*\n📌 *${selectedDl.quality}*\n📦 *${selectedDl.size}*\n\n🔗 *Direct Link:*\n${dlUrl}\n\n_IDM එකෙන් download කරන්න._\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: dlMek });
                    }

                    await socket.sendMessage(sender, {
                        text: `⏳ *Downloading to Server...*\n📌 *${selectedDl.quality}*\n📦 *Size:* ${selectedDl.size || 'N/A'}\n\n_කරුණාකර රැඳී සිටින්න..._`
                    }, { quoted: dlMek });

                    // ⭐ Download to server
                    await fs.ensureDir(TEMP_DIR);
                    const origName = selectedDl.title || `${cleanTmvTitle(movieData.title || chosenMovie.title)} - ${selectedDl.quality}.mkv`;
                    const cleanFileName = origName.replace(/[^a-zA-Z0-9 ._-]/g, '').trim().substring(0, 80) || 'movie.mkv';
                    const localFile = path.join(TEMP_DIR, `${Date.now()}_${cleanFileName}`);

                    try {
                        await downloadToServer(dlUrl, localFile);

                        const stats = await fs.stat(localFile);
                        const realSizeMB = stats.size / 1024 / 1024;

                        // ⚠️ Error page check
                        if (realSizeMB < 1) {
                            await fs.remove(localFile).catch(() => {});
                            throw new Error('Download failed — file too small (error page detected)');
                        }

                        await socket.sendMessage(sender, {
                            text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                        }, { quoted: dlMek });

                        // ⭐ Send as document
                        const mimeType = cleanFileName.toLowerCase().endsWith('.mkv') ? 'video/x-matroska' : 'video/mp4';

                        try {
                            await socket.sendMessage(sender, {
                                document: { url: localFile },
                                mimetype: mimeType,
                                fileName: cleanFileName,
                                caption: `✅ *1TAMILMV*\n\n🎬 *Title:* ${cleanTmvTitle(movieData.title || chosenMovie.title)}\n📌 *Quality:* ${selectedDl.quality}\n📦 *Size:* ${selectedDl.size || 'N/A'}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            }, { quoted: dlMek });

                            await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });

                        } catch (sendErr) {
                            await socket.sendMessage(sender, {
                                text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${dlUrl}\n\n_IDM එකෙන් download කරන්න._`
                            }, { quoted: dlMek });
                        }

                        // Cleanup
                        await fs.remove(localFile).catch(() => {});

                    } catch (downloadErr) {
                        console.error('1TamilMV download error:', downloadErr.message);
                        await socket.sendMessage(sender, {
                            text: `❌ *Download Error:* _${downloadErr.message}_\n\n🔗 *Direct Link:*\n${dlUrl}\n\n💡 _IDM එකෙන් download කරන්න._`
                        }, { quoted: dlMek });

                        try { await fs.remove(localFile); } catch {}
                    }
                };

                tmvDownloadListener = handleDownloadSelection;
                socket.ev.on('messages.upsert', tmvDownloadListener);

            } catch (infoErr) {
                clearAllTmvListeners();
                
                let errMsg = infoErr.message;
                if (errMsg.includes('timeout')) errMsg = 'API එක slow නිසා timeout වුනා. නැවත try කරන්න.';
                
                await socket.sendMessage(sender, { text: `❌ 1TamilMV Info Error: ${errMsg}` }, { quoted: replyMek });
            }
        };

        tmvSelectionListener = handleMovieSelection;
        socket.ev.on('messages.upsert', tmvSelectionListener);

    } catch (err) {
        clearAllTmvListeners();
        
        let errMsg = err.message;
        if (errMsg.includes('timeout')) errMsg = 'API එක slow නිසා timeout වුනා. නැවත try කරන්න.';
        
        await socket.sendMessage(sender, {
            text: `❌ Error: ${errMsg}`
        }, { quoted: msg });
    }
    break;
}
// ==========================================
// CINEMX - Movie Downloader (Fixed)
// ==========================================
case 'cinemx':
case 'cmx': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .cinemx Vishwanath and Sons*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const movieQuery = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/movies/cinemx';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';
    const TEMP_DIR = './tmp_cinemx';

    // ⏱️ TIMEOUTS
    const TIMEOUT_API = 120000;
    const TIMEOUT_INFO = 120000;

    let cmxSelectionListener = null;
    let cmxDownloadListener = null;
    let cmxMasterTimeout = null;

    const clearAllCmxListeners = () => {
        if (cmxSelectionListener) { socket.ev.off('messages.upsert', cmxSelectionListener); cmxSelectionListener = null; }
        if (cmxDownloadListener)  { socket.ev.off('messages.upsert', cmxDownloadListener);  cmxDownloadListener  = null; }
        if (cmxMasterTimeout)     { clearTimeout(cmxMasterTimeout); cmxMasterTimeout = null; }
    };

    const cleanCmxTitle = (t = '') =>
        t.replace(/\s*\|\s*සිංහල උපසිරැසි.*$/i, '')
         .replace(/\s*With Sinhala Subtitles.*$/i, '')
         .replace(/\s*\[.*$/i, '')
         .trim();

    const stripHtml = (t = '') => t.replace(/<[^>]*>/g, '').trim();

    const parseSizeMB = (sizeStr) => {
        if (!sizeStr) return 0;
        const m = sizeStr.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    // ⭐ Download to server
    const downloadToServer = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 5,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://cinemx.lk/',
                'Accept': '*/*'
            }
        });
        res.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching on CineMX...' }, { quoted: msg });

        // ═══ STEP 1 : SEARCH ═══
        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: movieQuery, api_key: API_KEY },
            timeout: TIMEOUT_API
        });

        const searchData = searchRes.data;
        const results = searchData.data || [];

        if (!searchData.status || results.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage('❌ NO RESULTS', '*කිසිදු චිත්‍රපටයක් හමු නොවීය!*', `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
            }, { quoted: msg });
            break;
        }

        const list = results.slice(0, 20);
        let listText = `🎬 *𝗖𝗜𝗡𝗘𝗠𝗫 𝗦𝗘𝗔𝗥𝗖𝗛 : _${movieQuery}_*\n╭──────●➤\n*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        list.forEach((item, index) => {
            const quality = stripHtml(item.quality) || 'HD';
            listText += `*🎥 ${index + 1} ┃❭❭ ${cleanCmxTitle(item.title)}*\n    ↳ (${item.year || 'N/A'} | ⭐ ${item.rating || 'N/A'} | ${quality})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: list[0].image || list[0].original_image || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;
        cmxMasterTimeout = setTimeout(clearAllCmxListeners, 180000);

        // ═══ STEP 2 : USER PICKS A MOVIE ═══
        const handleMovieSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            if (replyMek.message.extendedTextMessage?.contextInfo?.stanzaId !== searchMsgID) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= list.length) {
                return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${list.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: replyMek });
            }

            if (cmxSelectionListener) { socket.ev.off('messages.upsert', cmxSelectionListener); cmxSelectionListener = null; }

            const chosen = list[choice];
            await socket.sendMessage(sender, { text: '⏳ Fetching movie details & downloads...' }, { quoted: replyMek });

            try {
                // ⭐ 1. URL VALIDATE කරන්න
                let infoUrl = chosen.link || chosen.url;
                
                // ⚠️ Proxy URL reject
                if (!infoUrl || infoUrl.includes('/image/proxy') || infoUrl.includes('proxy.jpg')) {
                    infoUrl = chosen.url || chosen.original_url || chosen.page_url;
                }
                
                // ⚠️ cinemx.lk URL එකක් වෙන්න ඕන
                if (!infoUrl || !infoUrl.includes('cinemx.lk')) {
                    throw new Error('Valid CineMX URL එකක් හමු නොවීය. නැවත try කරන්න.');
                }
                
                console.log(`[CineMX] Fetching info from: ${infoUrl}`);

                // ⭐ 2. INFO API CALL
                const infoRes = await axios.get(`${API_BASE}/infodl`, {
                    params: { q: infoUrl, api_key: API_KEY },
                    timeout: TIMEOUT_INFO
                });

                const movie = infoRes.data?.data;
                const allDownloads = movie?.downloads || [];
                if (!movie || allDownloads.length === 0) throw new Error('බාගත කිරීමේ links හමු නොවීය.');

                let infoText = `🎬 *${cleanCmxTitle(movie.title)}*\n\n`;
                infoText += `📅 *Year:* ${movie.year || 'N/A'}\n`;
                infoText += `⭐ *IMDb:* ${movie.imdb || 'N/A'}\n`;
                if (movie.duration) infoText += `⏱️ *Duration:* ${movie.duration}\n`;
                if (movie.country) infoText += `🌍 *Country:* ${movie.country}\n`;
                if (movie.language) infoText += `🗣️ *Language:* ${movie.language}\n`;
                if (movie.director) infoText += `🎬 *Director:* ${movie.director}\n`;
                if (movie.genres?.length) infoText += `🎭 *Genres:* ${movie.genres.join(', ')}\n`;
                if (movie.cast?.length) infoText += `👥 *Cast:* ${movie.cast.slice(0, 5).join(', ')}${movie.cast.length > 5 ? '...' : ''}\n`;
                infoText += `\n`;

                if (movie.story) {
                    infoText += `📖 *Story:*\n_${movie.story.substring(0, 250)}..._\n\n`;
                }

                infoText += `*Available Downloads:*\n`;
                allDownloads.forEach((dl, i) => {
                    const sizeMB = parseSizeMB(dl.size);
                    const isSub = dl.quality === 'SRT' || dl.name?.toLowerCase().includes('subtitle');
                    let note = '';
                    if (isSub) note = ' 📝';
                    else if (sizeMB > 2000) note = ' ⚠️';
                    else note = ' ✓';
                    infoText += `*${i + 1}.* ${dl.name}${note}\n`;
                });
                infoText += `\n👉 *බාගත කිරීමට අදාළ අංකය Reply කරන්න.*\n_✓ = Video • 📝 = Subtitle • ⚠️ = 2GB+_`;

                const infoMsg = await socket.sendMessage(sender, {
                    image: { url: movie.image || chosen.image || pickBotImage(sessionConfig) },
                    caption: infoText
                }, { quoted: replyMek });

                const infoMsgID = infoMsg.key.id;

                // ═══ STEP 3 : USER PICKS A DOWNLOAD ═══
                const handleDownloadSelection = async ({ messages: dlMessages }) => {
                    const dlMek = dlMessages?.[0];
                    if (!dlMek?.message || dlMek.key.remoteJid !== sender) return;

                    const dlChoiceText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                    if (dlMek.message.extendedTextMessage?.contextInfo?.stanzaId !== infoMsgID) return;

                    const dlIdx = parseInt(dlChoiceText) - 1;
                    if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= allDownloads.length) {
                        return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${allDownloads.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: dlMek });
                    }

                    clearAllCmxListeners();
                    const selectedDl = allDownloads[dlIdx];
                    const dlUrl = selectedDl.link;
                    const sizeMB = parseSizeMB(selectedDl.size);
                    const isSubtitle = selectedDl.quality === 'SRT' || selectedDl.name?.toLowerCase().includes('subtitle');

                    await socket.sendMessage(sender, { react: { text: '📥', key: dlMek.key } });

                    // 📝 Subtitle → link only
                    if (isSubtitle) {
                        return socket.sendMessage(sender, {
                            text: `📝 *SUBTITLE FILE*\n\n🎬 *${cleanCmxTitle(movie.title)}*\n📌 *${selectedDl.name}*\n\n🔗 *Download Link:*\n${dlUrl}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: dlMek });
                    }

                    // ⚠️ 2GB ට වඩා ලොකු නම් → link only
                    if (sizeMB > 2000) {
                        return socket.sendMessage(sender, {
                            text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${cleanCmxTitle(movie.title)}*\n📌 *${selectedDl.quality}*\n📦 *${selectedDl.size}*\n\n🔗 *Direct Link:*\n${dlUrl}\n\n_IDM එකෙන් download කරන්න._\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: dlMek });
                    }

                    await socket.sendMessage(sender, {
                        text: `⏳ *Downloading to Server...*\n📌 *${selectedDl.quality}*\n📦 *Size:* ${selectedDl.size || 'N/A'}\n\n_කරුණාකර රැඳී සිටින්න..._`
                    }, { quoted: dlMek });

                    // ⭐ Download to server
                    await fs.ensureDir(TEMP_DIR);
                    const safeName = cleanCmxTitle(movie.title).replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 50);
                    const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.mp4`);

                    try {
                        await downloadToServer(dlUrl, localFile);

                        const stats = await fs.stat(localFile);
                        const realSizeMB = stats.size / 1024 / 1024;

                        // ⚠️ Error page check
                        if (realSizeMB < 1) {
                            await fs.remove(localFile).catch(() => {});
                            throw new Error('Download failed — file too small (error page detected)');
                        }

                        await socket.sendMessage(sender, {
                            text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                        }, { quoted: dlMek });

                        // ⭐ Send as document
                        const fileName = `${safeName} - ${selectedDl.quality}.mp4`;

                        try {
                            await socket.sendMessage(sender, {
                                document: { url: localFile },
                                mimetype: 'video/mp4',
                                fileName: fileName,
                                caption: `✅ *CINEMX MOVIE*\n\n🎬 *Title:* ${cleanCmxTitle(movie.title)}\n📅 *Year:* ${movie.year || 'N/A'}\n⭐ *IMDb:* ${movie.imdb || 'N/A'}\n📌 *Quality:* ${selectedDl.quality}\n📦 *Size:* ${selectedDl.size || 'N/A'}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            }, { quoted: dlMek });

                            await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });

                        } catch (sendErr) {
                            await socket.sendMessage(sender, {
                                text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${dlUrl}\n\n_IDM එකෙන් download කරන්න._`
                            }, { quoted: dlMek });
                        }

                        // Cleanup
                        await fs.remove(localFile).catch(() => {});

                    } catch (downloadErr) {
                        console.error('[CineMX] download error:', downloadErr.message);
                        await socket.sendMessage(sender, {
                            text: `❌ *Download Error:* _${downloadErr.message}_\n\n🔗 *Direct Link:*\n${dlUrl}\n\n💡 _IDM එකෙන් download කරන්න._`
                        }, { quoted: dlMek });

                        try { await fs.remove(localFile); } catch {}
                    }
                };

                cmxDownloadListener = handleDownloadSelection;
                socket.ev.on('messages.upsert', cmxDownloadListener);

            } catch (infoErr) {
                clearAllCmxListeners();
                
                let errMsg = infoErr.message;
                if (errMsg.includes('timeout')) errMsg = 'API එක slow නිසා timeout වුනා. නැවත try කරන්න.';
                if (errMsg.includes('proxy') || errMsg.includes('Invalid')) errMsg = 'මෙම result එකේ වැරදි link එකක් තියෙනවා. නැවත වෙන result එකක් try කරන්න.';
                
                await socket.sendMessage(sender, { text: `❌ CineMX Info Error: ${errMsg}` }, { quoted: replyMek });
            }
        };

        cmxSelectionListener = handleMovieSelection;
        socket.ev.on('messages.upsert', cmxSelectionListener);

    } catch (err) {
        clearAllCmxListeners();
        
        let errMsg = err.message;
        if (errMsg.includes('timeout')) errMsg = 'API එක slow නිසා timeout වුනා. නැවත try කරන්න.';
        
        await socket.sendMessage(sender, {
            text: `❌ Error: ${errMsg}`
        }, { quoted: msg });
    }
    break;
}
// ==========================================
// NETHMV - Stream Vault API Movie Bot
// ==========================================
case 'nethmv':
case 'vault': {
    const DEFAULT_FOOTER = `\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;
    const TEMP_DIR = './tmp_nethmv';

    // ⭐ API Config
    const VAULT_API_BASE = 'https://stream-vault-api.lovable.app/api/public/v1';
    const VAULT_API_KEY = 'mvk_796aa89023bfa0a1bca326895f5c30495064a557c948674b';

    if (!args.length) {
        return socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර search කිරීමට keyword එකක් දෙන්න!*\n\n*📌 Usage:* `.nethmv action movie`\n*📌 Usage:* `.nethmv spider*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }

    const searchQuery = args.join(' ').trim();

    let nethSelectionListener = null;
    let nethDownloadListener = null;
    let nethMasterTimeout = null;

    const clearAllNethListeners = () => {
        if (nethSelectionListener) { socket.ev.off('messages.upsert', nethSelectionListener); nethSelectionListener = null; }
        if (nethDownloadListener)  { socket.ev.off('messages.upsert', nethDownloadListener);  nethDownloadListener  = null; }
        if (nethMasterTimeout)     { clearTimeout(nethMasterTimeout); nethMasterTimeout = null; }
    };

    const formatBytes = (bytes) => {
        if (!bytes || bytes === 0) return 'Unknown';
        const mb = bytes / 1024 / 1024;
        if (mb < 1024) return `${mb.toFixed(1)} MB`;
        return `${(mb / 1024).toFixed(2)} GB`;
    };

    const parseSizeMB = (s) => {
        if (!s) return 0;
        if (typeof s === 'number') return s / 1024 / 1024;
        const m = s.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    // ⭐ Server download helper
    const downloadToServer = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 10,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*'
            }
        });

        const ct = (res.headers['content-type'] || '').toLowerCase();
        if (ct.includes('text/html')) {
            res.data.destroy();
            throw new Error('HTML response (not a file)');
        }

        res.data.pipe(writer);
        await new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });

        return await fs.stat(dest);
    };

    try {
        await socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗦𝗘𝗔𝗥𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🔍 *Searching Stream Vault for:* _${searchQuery}_\n⚡ _Please wait..._`
        }, { quoted: msg });

        // ═══ STEP 1 : SEARCH ═══
        const searchRes = await axios.get(`${VAULT_API_BASE}/search`, {
            params: { q: searchQuery, limit: 20 },
            headers: { 'x-api-key': VAULT_API_KEY },
            timeout: 120000
        });

        const searchData = searchRes.data;
        const files = searchData.files || [];

        if (!searchData.count || files.length === 0) {
            return socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    `*"${searchQuery}"* සඳහා කිසිදු file එකක් හමු නොවීය!`,
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
        }

        let listText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗡𝗘𝗧𝗛𝗠𝗩 ❫*\n\n🎯 *Query:* _${searchQuery}_\n📊 *Results:* _${files.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;

        files.forEach((file, index) => {
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            const sizeMB = parseSizeMB(file.size_bytes);
            let sizeStr = 'Unknown';
            if (sizeMB > 0) {
                sizeStr = sizeMB > 1024 ? `${(sizeMB / 1024).toFixed(2)} GB` : `${sizeMB.toFixed(1)} MB`;
            }
            listText += `*${num}* ➜ 🎬 _${file.name || file.filename}_*\n    ↳ _(${file.type || 'file'} | ${sizeStr})_\n`;
        });
        listText += `\n📌 _Reply with number to download!_${DEFAULT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, { text: listText }, { quoted: msg });
        const searchMsgID = searchMsg.key.id;

        nethMasterTimeout = setTimeout(clearAllNethListeners, 180000);

        // ═══ STEP 2 : USER PICKS FILE ═══
        const handleNethSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;
            if (!isReply) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= files.length) {
                return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${files.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: replyMek });
            }

            if (nethSelectionListener) { socket.ev.off('messages.upsert', nethSelectionListener); nethSelectionListener = null; }

            const chosenFile = files[choice];

            await socket.sendMessage(sender, { react: { text: '📥', key: replyMek.key } });
            await socket.sendMessage(sender, {
                text: `⏳ *Fetching file details...*\n\n🎬 *${chosenFile.name}*\n📦 *Size:* ${formatBytes(chosenFile.size_bytes)}\n\n_කරුණාකර රැඳී සිටින්න..._`
            }, { quoted: replyMek });

            try {
                // ═══ STEP 3 : Get fresh download link ═══
                let fileData = chosenFile;

                // Fresh link එකක් ඕන නම් — `/files/:id` call කරන්න
                try {
                    const freshRes = await axios.get(`${VAULT_API_BASE}/files/${chosenFile.id}`, {
                        headers: { 'x-api-key': VAULT_API_KEY },
                        timeout: 120000
                    });
                    if (freshRes.data && freshRes.data.download_url) {
                        fileData = freshRes.data;
                    }
                } catch (freshErr) {
                    console.log('[NethMV] Fresh link fetch failed, using search link:', freshErr.message);
                }

                const dlUrl = fileData.download_url || fileData.stream_url;
                const sizeMB = parseSizeMB(fileData.size_bytes);
                const isVideo = (fileData.mime_type || '').includes('video');
                const fileName = fileData.filename || `${fileData.name || 'file'}.mp4`;

                if (!dlUrl) {
                    throw new Error('Download link එක හමු නොවීය.');
                }

                // ⚠️ 2GB limit → link only
                if (sizeMB > 2000) {
                    return socket.sendMessage(sender, {
                        text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${fileData.name}*\n📦 *Size:* ${formatBytes(fileData.size_bytes)}\n\n🔗 *Direct Link:*\n${dlUrl}\n\n_IDM එකෙන් download කරන්න._${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                }

                // ⭐ Server download
                await fs.ensureDir(TEMP_DIR);
                const safeName = (fileData.name || 'file').replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 50);
                const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}${path.extname(fileName) || '.mp4'}`);

                await downloadToServer(dlUrl, localFile);

                const stats = await fs.stat(localFile);
                const realSizeMB = stats.size / 1024 / 1024;

                if (realSizeMB < 1) {
                    await fs.remove(localFile).catch(() => {});
                    throw new Error('File too small (error page)');
                }

                await socket.sendMessage(sender, {
                    text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                }, { quoted: replyMek });

                // ⭐ Send as document
                try {
                    await socket.sendMessage(sender, {
                        document: { url: localFile },
                        mimetype: isVideo ? 'video/mp4' : 'application/octet-stream',
                        fileName: fileName,
                        caption: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗡𝗘𝗧𝗛𝗠𝗩*\n\n🎬 *Name:* ${fileData.name}\n📝 *Description:* ${(fileData.description || 'N/A').substring(0, 100)}\n📦 *Size:* ${formatBytes(fileData.size_bytes)}\n🏷️ *Type:* ${fileData.type || 'file'}\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
                    }, { quoted: replyMek });

                    await socket.sendMessage(sender, { react: { text: '✅', key: replyMek.key } });

                } catch (sendErr) {
                    await socket.sendMessage(sender, {
                        text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${dlUrl}${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                }

                // Cleanup
                await fs.remove(localFile).catch(() => {});

            } catch (err) {
                console.error('[NethMV] download error:', err.message);
                clearAllNethListeners();

                await socket.sendMessage(sender, {
                    text: `⚠️ *Direct Download*\n\n🎬 *${chosenFile.name}*\n📦 *Size:* ${formatBytes(chosenFile.size_bytes)}\n\n🔗 *Download Link:*\n${chosenFile.download_url || chosenFile.stream_url || 'N/A'}\n\n💡 _IDM එකෙන් download කරන්න._${DEFAULT_FOOTER}`
                }, { quoted: replyMek });
            }
        };

        nethSelectionListener = handleNethSelection;
        socket.ev.on('messages.upsert', nethSelectionListener);

    } catch (err) {
        clearAllNethListeners();
        console.error('[NethMV] error:', err.message);

        let errMsg = err.message;
        if (errMsg.includes('401')) errMsg = 'API Key එක වැරදියි.';
        else if (errMsg.includes('403')) errMsg = 'API Key එක revoke කරලා.';
        else if (errMsg.includes('429')) errMsg = 'Rate limit ඉක්මවා ඇත. පසුව try කරන්න.';
        else if (errMsg.includes('500')) errMsg = 'Server error. නැවත try කරන්න.';

        await socket.sendMessage(sender, {
            text: `❌ *NethMV Error:* _${errMsg}_${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }
    break;
}
// ==========================================
// MOVIEMANIALK - Fixed (Server Download)
// ==========================================
case 'moviemania':
case 'mm':
case 'mmlk': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .moviemania 13 Days*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const mmQuery = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/movies/moviemanialk';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';
    const TEMP_DIR = './tmp_moviemania';

    // ⏱️ TIMEOUTS
    const TIMEOUT_API = 120000;
    const TIMEOUT_INFO = 120000;

    let mmSelectionListener = null;
    let mmDownloadListener = null;
    let mmMasterTimeout = null;

    const clearAllMmListeners = () => {
        if (mmSelectionListener) { socket.ev.off('messages.upsert', mmSelectionListener); mmSelectionListener = null; }
        if (mmDownloadListener)  { socket.ev.off('messages.upsert', mmDownloadListener);  mmDownloadListener  = null; }
        if (mmMasterTimeout)     { clearTimeout(mmMasterTimeout); mmMasterTimeout = null; }
    };

    const parseSizeMB = (sizeStr) => {
        if (!sizeStr) return 0;
        const m = sizeStr.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    // ⭐ Download to server
    const downloadToServer = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 5,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://www.moviemanialk.com/',
                'Accept': '*/*'
            }
        });
        res.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching on MovieManiaLK...' }, { quoted: msg });

        // ═══ STEP 1 : SEARCH ═══
        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: mmQuery, api_key: API_KEY },
            timeout: TIMEOUT_API
        });

        const searchData = searchRes.data;
        const results = searchData.results || [];

        if (!searchData.status || results.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage('❌ NO RESULTS', '*කිසිදු චිත්‍රපටයක් හමු නොවීය!*', `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
            }, { quoted: msg });
            break;
        }

        const list = results.slice(0, 20);
        let listText = `🎬 *𝗠𝗢𝗩𝗜𝗘𝗠𝗔𝗡𝗜𝗔𝗟𝗞 𝗦𝗘𝗔𝗥𝗖𝗛 : _${mmQuery}_*\n╭──────●➤\n*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        list.forEach((item, index) => {
            const typeIcon = item.type === 'tv' ? '📺' : '🎥';
            listText += `*${typeIcon} ${index + 1} ┃❭❭ ${item.title}*\n    ↳ (${item.year || 'N/A'} | ⭐ ${item.rating || 'N/A'} | ${item.quality || 'HD'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: list[0].poster || list[0].thumbnail || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;
        mmMasterTimeout = setTimeout(clearAllMmListeners, 180000);

        // ═══ STEP 2 : USER PICKS ═══
        const handleMovieSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            if (replyMek.message.extendedTextMessage?.contextInfo?.stanzaId !== searchMsgID) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= list.length) {
                return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${list.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: replyMek });
            }

            if (mmSelectionListener) { socket.ev.off('messages.upsert', mmSelectionListener); mmSelectionListener = null; }

            const chosen = list[choice];
            await socket.sendMessage(sender, { text: '⏳ Fetching details & download links...' }, { quoted: replyMek });

            try {
                // ═══ STEP 3 : INFO + DL API ═══
                const infoRes = await axios.get(`${API_BASE}/infodl`, {
                    params: { q: chosen.url, api_key: API_KEY },
                    timeout: TIMEOUT_INFO
                });

                const movie = infoRes.data?.result;
                const allDownloads = movie?.downloads || [];
                if (!movie || allDownloads.length === 0) throw new Error('බාගත කිරීමේ links හමු නොවීය.');

                let infoText = `🎬 *${movie.title}*\n\n`;
                infoText += `📅 *Year:* ${movie.year || 'N/A'}\n`;
                infoText += `⭐ *Rating:* ${movie.rating || 'N/A'}\n`;
                infoText += `🎞 *Quality:* ${movie.quality || 'N/A'}\n`;
                if (movie.runtime) infoText += `⏱️ *Runtime:* ${movie.runtime}\n`;
                if (movie.director) infoText += `🎬 *Director:* ${movie.director}\n`;
                if (movie.country) infoText += `🌍 *Country:* ${movie.country}\n`;
                if (movie.genres?.length) infoText += `🎭 *Genres:* ${movie.genres.join(', ')}\n`;
                if (movie.cast) infoText += `👥 *Cast:* ${movie.cast.substring(0, 80)}...\n`;
                infoText += `\n`;

                if (movie.description) {
                    infoText += `📖 *Story:*\n_${movie.description.substring(0, 250)}..._\n\n`;
                }

                infoText += `*Available Downloads:*\n`;
                allDownloads.forEach((dl, i) => {
                    const sizeMB = parseSizeMB(dl.size);
                    const isSub = dl.quality?.toLowerCase().includes('subtitle') || dl.server === 'Subtitle Server';
                    let note = '';
                    if (isSub) note = ' 📝';
                    else if (sizeMB > 2000) note = ' ⚠️';
                    else note = ' ✓';
                    infoText += `*${i + 1}.* ${dl.quality}${note}\n`;
                });
                infoText += `\n👉 *බාගත කිරීමට අදාළ අංකය Reply කරන්න.*\n_✓ = Video • 📝 = Subtitle • ⚠️ = 2GB+_`;

                const infoMsg = await socket.sendMessage(sender, {
                    image: { url: movie.image || movie.poster || chosen.poster || pickBotImage(sessionConfig) },
                    caption: infoText
                }, { quoted: replyMek });

                const infoMsgID = infoMsg.key.id;

                // ═══ STEP 4 : USER PICKS DOWNLOAD ═══
                const handleDownloadSelection = async ({ messages: dlMessages }) => {
                    const dlMek = dlMessages?.[0];
                    if (!dlMek?.message || dlMek.key.remoteJid !== sender) return;

                    const dlChoiceText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                    if (dlMek.message.extendedTextMessage?.contextInfo?.stanzaId !== infoMsgID) return;

                    const dlIdx = parseInt(dlChoiceText) - 1;
                    if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= allDownloads.length) {
                        return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${allDownloads.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: dlMek });
                    }

                    clearAllMmListeners();
                    const selectedDl = allDownloads[dlIdx];
                    const dlUrl = selectedDl.proxy_link || selectedDl.direct_link || selectedDl.link;
                    const sizeMB = parseSizeMB(selectedDl.size);
                    const isSubtitle = selectedDl.quality?.toLowerCase().includes('subtitle') || selectedDl.server === 'Subtitle Server';

                    await socket.sendMessage(sender, { react: { text: '📥', key: dlMek.key } });

                    // 📝 Subtitle → link only
                    if (isSubtitle) {
                        return socket.sendMessage(sender, {
                            text: `📝 *SINHALA SUBTITLE*\n\n🎬 *${movie.title}*\n📌 *${selectedDl.quality}*\n\n🔗 *Download Link:*\n${dlUrl}\n\n_IDM එකෙන් download කරන්න._\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: dlMek });
                    }

                    // ⚠️ 2GB ට වඩා ලොකු නම් → link only
                    if (sizeMB > 2000) {
                        return socket.sendMessage(sender, {
                            text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${movie.title}*\n📌 *${selectedDl.quality}*\n📦 *${selectedDl.size}*\n\n🔗 *Direct Link:*\n${dlUrl}\n\n_IDM එකෙන් download කරන්න._\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: dlMek });
                    }

                    await socket.sendMessage(sender, {
                        text: `⏳ *Downloading to Server...*\n📌 *${selectedDl.quality}*\n📦 *Size:* ${selectedDl.size || 'N/A'}\n\n_කරුණාකර රැඳී සිටින්න..._`
                    }, { quoted: dlMek });

                    // ⭐ Download to server
                    await fs.ensureDir(TEMP_DIR);
                    const safeName = movie.title.replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 50);
                    const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.mp4`);

                    try {
                        await downloadToServer(dlUrl, localFile);

                        const stats = await fs.stat(localFile);
                        const realSizeMB = stats.size / 1024 / 1024;

                        // ⚠️ Error page check
                        if (realSizeMB < 1) {
                            await fs.remove(localFile).catch(() => {});
                            throw new Error('Download failed — file too small (error page detected)');
                        }

                        await socket.sendMessage(sender, {
                            text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                        }, { quoted: dlMek });

                        // ⭐ Send as document
                        const fileName = `${safeName} - ${selectedDl.quality.replace(/[^a-zA-Z0-9 ]/g, '').trim()}.mp4`;

                        try {
                            await socket.sendMessage(sender, {
                                document: { url: localFile },
                                mimetype: 'video/mp4',
                                fileName: fileName,
                                caption: `✅ *MOVIEMANIALK*\n\n🎬 *Title:* ${movie.title}\n📅 *Year:* ${movie.year || 'N/A'}\n⭐ *Rating:* ${movie.rating || 'N/A'}\n📌 *Quality:* ${selectedDl.quality}\n📦 *Size:* ${selectedDl.size || 'N/A'}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            }, { quoted: dlMek });

                            await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });

                        } catch (sendErr) {
                            await socket.sendMessage(sender, {
                                text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${dlUrl}\n\n_IDM එකෙන් download කරන්න._`
                            }, { quoted: dlMek });
                        }

                        // Cleanup
                        await fs.remove(localFile).catch(() => {});

                    } catch (downloadErr) {
                        console.error('[MovieMania] download error:', downloadErr.message);
                        await socket.sendMessage(sender, {
                            text: `❌ *Download Error:* _${downloadErr.message}_\n\n🔗 *Direct Link:*\n${dlUrl}\n\n💡 _IDM එකෙන් download කරන්න._`
                        }, { quoted: dlMek });

                        try { await fs.remove(localFile); } catch {}
                    }
                };

                mmDownloadListener = handleDownloadSelection;
                socket.ev.on('messages.upsert', mmDownloadListener);

            } catch (infoErr) {
                clearAllMmListeners();
                
                let errMsg = infoErr.message;
                if (errMsg.includes('timeout')) errMsg = 'API එක slow නිසා timeout වුනා. නැවත try කරන්න.';
                
                await socket.sendMessage(sender, { text: `❌ MovieManiaLK Info Error: ${errMsg}` }, { quoted: replyMek });
            }
        };

        mmSelectionListener = handleMovieSelection;
        socket.ev.on('messages.upsert', mmSelectionListener);

    } catch (err) {
        clearAllMmListeners();
        
        let errMsg = err.message;
        if (errMsg.includes('timeout')) errMsg = 'API එක slow නිසා timeout වුනා. නැවත try කරන්න.';
        
        await socket.sendMessage(sender, {
            text: `❌ Error: ${errMsg}`
        }, { quoted: msg });
    }
    break;
}
case 'hexrom':
case 'rom':
case 'game': {
    const chatJid = msg.key.remoteJid;
    const DEFAULT_FOOTER = `\n\n> 🎮 𝗦𝗛??𝗚𝗚𝗬 𝗫𝗠𝗗 🎮\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;

    // ⚙️ CONFIG
    const HEXROM_CONFIG = {
        PART_SIZE_MB: 500,
        SEND_DELAY_MS: 180000,       // 3 minutes
        TEMP_DIR: './tmp_hexrom',
        MAX_PARTS: 30
    };

    const API_BASE = "https://api.chamindu.site";
    const API_KEY = "chama_api_11230a80e5eed3c1b80bfcc5d1773ec9";
    const DEFAULT_IMAGE = "https://images.unsplash.com/photo-1550745165-9bc0b252726f?w=500";

    function getCircledNumber(num) {
        const arr = ['①','②','③','④','⑤','⑥','⑦','⑧','⑨','⑩','⑪','⑫','⑬','⑭','⑮','⑯','⑰','⑱','⑲','⑳','㉑','㉒','㉓','㉔','㉕','㉖','㉗','㉘','㉙','㉚'];
        return arr[num - 1] || `[${num}]`;
    }

    // ============ HELPERS ============
    const hrSanitize = (n) => (n || 'rom').replace(/[^a-zA-Z0-9 _-]/g, '').trim().substring(0, 60);

    const hrFmtSize = (bytes) => {
        if (!bytes || bytes === 0) return 'Unknown';
        const mb = bytes / 1024 / 1024;
        if (mb < 1) return `${(bytes / 1024).toFixed(0)} KB`;
        if (mb < 1024) return `${mb.toFixed(1)} MB`;
        return `${(mb / 1024).toFixed(2)} GB`;
    };

    const hrGetSize = async (url) => {
        try {
            const r = await axios.head(url, { timeout: 120000, maxRedirects: 5, headers: { 'User-Agent': 'Mozilla/5.0' } });
            return parseInt(r.headers['content-length'] || '0');
        } catch (e) { return 0; }
    };

    const hrDownload = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const res = await axios.get(url, {
            responseType: 'stream', timeout: 0, maxRedirects: 5,
            headers: { 'User-Agent': 'Mozilla/5.0' }
        });
        const writer = fs.createWriteStream(dest);
        await pipeline(res.data, writer);
        return dest;
    };

    const hrSplit = async (srcPath, chunkMB, outDir, baseName) => {
        const chunkSize = chunkMB * 1024 * 1024;
        const stat = await fs.stat(srcPath);
        const totalSize = stat.size;
        const numChunks = Math.ceil(totalSize / chunkSize);
        const ext = path.extname(srcPath) || '.zip';

        await fs.ensureDir(outDir);
        const parts = [];

        const readStream = fs.createReadStream(srcPath, { highWaterMark: 4 * 1024 * 1024 });
        let currentChunk = 0;
        let currentSize = 0;
        let writeStream = null;
        let totalWritten = 0;

        const openChunk = () => {
            const p = path.join(outDir, `${baseName}.part${String(currentChunk + 1).padStart(3, '0')}${ext}`);
            writeStream = fs.createWriteStream(p);
            parts.push({ index: currentChunk + 1, path: p, size: 0 });
        };
        openChunk();

        for await (const chunk of readStream) {
            let offset = 0;
            while (offset < chunk.length) {
                const spaceLeft = chunkSize - currentSize;
                const toWrite = Math.min(spaceLeft, chunk.length - offset);
                const slice = chunk.subarray(offset, offset + toWrite);

                if (!writeStream.write(slice)) {
                    await new Promise(r => writeStream.once('drain', r));
                }
                currentSize += toWrite;
                totalWritten += toWrite;
                parts[parts.length - 1].size += toWrite;
                offset += toWrite;

                if (currentSize >= chunkSize && totalWritten < totalSize) {
                    await new Promise(r => writeStream.end(r));
                    currentChunk++;
                    currentSize = 0;
                    if (currentChunk < numChunks) openChunk();
                }
            }
        }

        if (writeStream && !writeStream.writableEnded) {
            await new Promise(r => writeStream.end(r));
        }
        return parts;
    };

    const hrAutoSendAll = async (romTitle, downloadUrl, socket, chatJid, replyMek) => {
        const safeTitle = hrSanitize(romTitle);
        const tmpRoot = path.join(HEXROM_CONFIG.TEMP_DIR, `${Date.now()}_${safeTitle}`);
        const rawDir = path.join(tmpRoot, 'raw');
        const splitDir = path.join(tmpRoot, 'split');
        await fs.ensureDir(rawDir);
        await fs.ensureDir(splitDir);

        try {
            await socket.sendMessage(chatJid, {
                text: `*❪ AUTO DOWNLOAD STARTED ❫*\n\n🎮 *${romTitle}*\n📦 *Chunk Size:* ${HEXROM_CONFIG.PART_SIZE_MB} MB\n⏱️ *Delay:* ${Math.round(HEXROM_CONFIG.SEND_DELAY_MS / 60000)} min\n\n⚡ _Starting now... Do NOT spam._\n> ⚠️ _This can take 30+ minutes._${DEFAULT_FOOTER}`
            }, { quoted: replyMek });

            // 1. Download raw ROM
            await socket.sendMessage(chatJid, { text: `📥 *Downloading ROM to server...*\n⚡ _This may take a while for large files._` });

            const rawFile = path.join(rawDir, `${safeTitle}.zip`);
            await hrDownload(downloadUrl, rawFile);
            const rawStat = await fs.stat(rawFile);

            await socket.sendMessage(chatJid, {
                text: `✅ *Downloaded!*\n📦 Size: *${hrFmtSize(rawStat.size)}*\n\n✂️ _Splitting into ${HEXROM_CONFIG.PART_SIZE_MB}MB chunks..._`
            });

            // 2. Split
            let chunks = [{ path: rawFile, size: rawStat.size, temp: false }];

            if (rawStat.size > HEXROM_CONFIG.PART_SIZE_MB * 1024 * 1024) {
                const splitParts = await hrSplit(rawFile, HEXROM_CONFIG.PART_SIZE_MB, splitDir, safeTitle);
                chunks = splitParts.map(c => ({ ...c, temp: true }));
                await fs.remove(rawFile).catch(() => {});
            }

            const totalParts = chunks.length;

            // 3. Send parts with delay
            await socket.sendMessage(chatJid, {
                text: `*❪ READY TO SEND ❫*\n\n📦 *Total Parts:* ${totalParts}\n💾 *Part Size:* ~${HEXROM_CONFIG.PART_SIZE_MB} MB\n⏱️ *Delay Between:* 3 min\n\n_Starting now..._`
            });

            let sentCount = 0;
            let failedCount = 0;

            for (let i = 0; i < totalParts; i++) {
                const chunk = chunks[i];
                const chunkLabel = `Part ${i + 1}/${totalParts}`;

                const ext = path.extname(chunk.path) || '.zip';
                const fileName = `${safeTitle}_Part${i + 1}${ext}`;

                try {
                    await socket.sendMessage(chatJid, {
                        document: { url: chunk.path },
                        mimetype: 'application/octet-stream',
                        fileName: fileName,
                        caption: `🎮 *${romTitle}*\n📌 *${chunkLabel}*\n📊 ${hrFmtSize(chunk.size)}${DEFAULT_FOOTER}`
                    });

                    sentCount++;
                    console.log(`[HexRom] ✅ Sent ${chunkLabel} (${hrFmtSize(chunk.size)})`);

                    if (chunk.temp) await fs.remove(chunk.path).catch(() => {});

                    const isVeryLast = (i === totalParts - 1);
                    if (!isVeryLast) {
                        await socket.sendMessage(chatJid, {
                            text: `✅ *${chunkLabel}* sent.\n⏱️ _Waiting 3 min for next part..._`
                        });
                        await new Promise(r => setTimeout(r, HEXROM_CONFIG.SEND_DELAY_MS));
                    }
                } catch (sendErr) {
                    console.error(`[HexRom] Send fail ${chunkLabel}:`, sendErr.message);
                    failedCount++;
                    await socket.sendMessage(chatJid, {
                        text: `❌ *${chunkLabel}* send failed!\n🔗 Direct:\n${downloadUrl}\n\n_${sendErr.message}_`
                    });
                }
            }

            await socket.sendMessage(chatJid, {
                text: `*❪ COMPLETE ✅ ❫*\n\n🎮 *${romTitle}*\n📦 *Sent:* ${sentCount} files\n❌ *Failed:* ${failedCount}\n\n💡 *Extract:* Put all parts in one folder → Open Part 1 with WinRAR/7-Zip → Extract ✅${DEFAULT_FOOTER}`
            }, { quoted: replyMek });

        } catch (err) {
            console.error('[HexRom] Auto fatal:', err);
            await socket.sendMessage(chatJid, {
                text: `❌ *Auto download failed!*\n_${err.message}_${DEFAULT_FOOTER}`
            });
        } finally {
            await fs.remove(tmpRoot).catch(() => {});
        }
    };

    // ============ VALIDATION ============
    if (!args.length) {
        await socket.sendMessage(chatJid, {
            text: `*❪ ERROR ❫*\n\n⚠️ *Invalid Usage!*\n\n🎮 *Example:*\n• .hexrom god of war\n• .rom gta san andreas\n\n📝 _Please provide the ROM name!_${DEFAULT_FOOTER}`
        }, { quoted: msg });
        break;
    }

    const romQuery = args.join(' ');
    await socket.sendMessage(chatJid, {
        text: `*❪ SEARCHING ❫*\n\n🔍 *Searching HexRom...*\n⚡ _Please wait a moment._`
    });

    // ============ SEARCH ============
    let searchResponse = null;
    let searchRetries = 3;
    while (searchRetries > 0 && !searchResponse) {
        try {
            searchResponse = await axios.get(`${API_BASE}/api/v1/games/hexrom/search?q=${encodeURIComponent(romQuery)}&api_key=${API_KEY}`, { timeout: 120000 });
        } catch (searchErr) {
            searchRetries--;
            if (searchRetries === 0) throw searchErr;
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
    }
    const searchData = searchResponse.data;
    const resultsList = searchData.data || [];

    try {
        if (!searchData.status || resultsList.length === 0) {
            await socket.sendMessage(chatJid, {
                text: `*❪ NO RESULTS ❫*\n\n😞 *No ROMs Found!*\n\n🎮 *Query:* _${romQuery}_\n💡 *Tip:* _Check spelling and try again!_${DEFAULT_FOOTER}`
            }, { quoted: msg });
            break;
        }

        const romResults = resultsList.slice(0, 25);
        let listText = `*❪ HEXROM SEARCH RESULTS ❫*\n\n🎯 *Query:* _${romQuery}_\n📊 *Results:* _${romResults.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;

        romResults.forEach((item, index) => {
            const num = getCircledNumber(index + 1);
            listText += `${num} ➜ 🎮 _${item.title.substring(0, 45)}_\n\n`;
        });
        listText += `${DEFAULT_FOOTER}`;

        const sentMsg = await socket.sendMessage(chatJid, {
            image: { url: romResults[0].image || DEFAULT_IMAGE },
            caption: listText
        }, { quoted: msg });
        const messageID = sentMsg.key.id;

        const originalSenderNumber = (msg.key.participant || msg.key.remoteJid || '').split('@')[0].split(':')[0];

        // Listener registry (memory leak fix)
        if (!global.__hexromListeners) global.__hexromListeners = new Map();
        if (!global.__hexromDispatcher) {
            global.__hexromDispatcher = true;
            socket.ev.on('messages.upsert', async ({ messages }) => {
                const m = messages[0];
                if (!m?.message) return;
                const stanzaId = m.message.extendedTextMessage?.contextInfo?.stanzaId;
                if (!stanzaId) return;
                const entry = global.__hexromListeners.get(stanzaId);
                if (entry) {
                    try { await entry.handler({ messages }); }
                    catch (e) { console.error('[HexRom] handler error:', e); }
                }
            });
        }

        // ============ SELECTION HANDLER ============
        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || "").trim();
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;
            const replierNumber = (replyMek.key.participant || replyMek.key.remoteJid || '').split('@')[0].split(':')[0];
            const isSameUser = replierNumber === originalSenderNumber;
            const isSameChat = replyMek.key.remoteJid === chatJid;

            if (isReplyToSentMsg && isSameChat && isSameUser) {
                clearTimeout(cleanupTimeout);
                global.__hexromListeners.delete(messageID);

                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= romResults.length) {
                    return socket.sendMessage(chatJid, {
                        text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 - ${romResults.length}_${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                }

                const selectedItem = romResults[choice];
                const romTargetUrl = selectedItem.link;

                await socket.sendMessage(chatJid, {
                    text: `*❪ FETCHING ❫*\n\n🎮 *Fetching Download Links...*\n⚡ _Please wait..._`
                }, { quoted: replyMek });

                let dlResponse = null;
                let dlRetries = 3;
                while (dlRetries > 0 && !dlResponse) {
                    try {
                        dlResponse = await axios.get(`${API_BASE}/api/v1/games/hexrom/download?q=${encodeURIComponent(romTargetUrl)}&api_key=${API_KEY}`, { timeout: 120000 });
                    } catch (dlErr) {
                        dlRetries--;
                        if (dlRetries === 0) throw dlErr;
                        await new Promise(resolve => setTimeout(resolve, 2000));
                    }
                }
                const dlData = dlResponse.data;

                try {
                    if (!dlData.status || !dlData.data) {
                        throw new Error('Failed to fetch ROM download links');
                    }

                    const romInfo = dlData.data;
                    const romTitle = romInfo.title || selectedItem.title;
                    const allDownloads = romInfo.downloads || [];

                    if (allDownloads.length === 0) {
                        return socket.sendMessage(chatJid, {
                            text: `*❪ NO DOWNLOADS ❫*\n\n⚠️ *No download links found!*${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                    }

                    let validDownloads = allDownloads.slice(0, HEXROM_CONFIG.MAX_PARTS).map(l => ({
                        name: l.name || 'ROM File',
                        size: l.size || 'Unknown',
                        link: l.download_link || l.link || l.url
                    }));

                    // Get sizes for accuracy
                    for (let i = 0; i < validDownloads.length; i++) {
                        if (!validDownloads[i].size || validDownloads[i].size === 'Unknown') {
                            const sz = await hrGetSize(validDownloads[i].link);
                            validDownloads[i].size = hrFmtSize(sz);
                        }
                    }

                    const romDetailsText = `*❪ ROM DETAILS ❫*\n\n🎮 *${romTitle}*\n📊 *Available Files:* ${validDownloads.length}${DEFAULT_FOOTER}`;

                    const romPosterUrl = romInfo.image || selectedItem.image || DEFAULT_IMAGE;
                    await socket.sendMessage(chatJid, {
                        image: { url: romPosterUrl },
                        caption: romDetailsText
                    }, { quoted: replyMek });

                    let downloadOptionsText = `*❪ ROM DOWNLOADS ❫*\n\n`;
                    downloadOptionsText += `*99* ➜ 📦 *AUTO DOWNLOAD & SEND ALL* (${HEXROM_CONFIG.PART_SIZE_MB}MB chunks, 3min delay)\n`;
                    downloadOptionsText += `*00* ➜ 📥 _Get ALL links at once_\n\n`;
                    downloadOptionsText += `*👇 Or Pick a Single File 👇*\n\n`;
                    validDownloads.forEach((dl, i) => {
                        const num = getCircledNumber(i + 1);
                        downloadOptionsText += `${num} ➜ 🔗 _${dl.name.substring(0, 50)}_\n📦 _Size: ${dl.size}_\n`;
                    });
                    downloadOptionsText += `\n*💬 REPLY TO GET LINK 💬*\n📌 _99 = Auto-send • 00 = All links • 1-${validDownloads.length} = Single file_${DEFAULT_FOOTER}`;

                    const downloadOptionsMsg = await socket.sendMessage(chatJid, { text: downloadOptionsText }, { quoted: replyMek });
                    const optionsMsgID = downloadOptionsMsg.key.id;

                    // ============ DOWNLOAD HANDLER ============
                    const handleDownloadEvent = async ({ messages: downloadMessages }) => {
                        const downloadMek = downloadMessages[0];
                        if (!downloadMek?.message) return;

                        const downloadChoice = (downloadMek.message.conversation || downloadMek.message.extendedTextMessage?.text || "").trim();
                        const isReplyToOptionsMsg = downloadMek.message.extendedTextMessage?.contextInfo?.stanzaId === optionsMsgID;
                        const dlReplierNumber = (downloadMek.key.participant || downloadMek.key.remoteJid || '').split('@')[0].split(':')[0];
                        const isSameDlUser = dlReplierNumber === originalSenderNumber;
                        const isSameDlChat = downloadMek.key.remoteJid === chatJid;

                        if (isReplyToOptionsMsg && isSameDlChat && isSameDlUser) {
                            clearTimeout(dlCleanupTimeout);
                            global.__hexromListeners.delete(optionsMsgID);

                            // ===== 99: AUTO DOWNLOAD & SEND ALL =====
                            if (downloadChoice === '99') {
                                await socket.sendMessage(chatJid, { react: { text: '📦', key: downloadMek.key } });
                                const firstDownload = validDownloads[0];
                                await hrAutoSendAll(romTitle, firstDownload.link, socket, chatJid, downloadMek);
                                return;
                            }

                            // ===== 00: All links =====
                            if (downloadChoice === '0' || downloadChoice === '00') {
                                await socket.sendMessage(chatJid, { react: { text: '📥', key: downloadMek.key } });
                                await socket.sendMessage(chatJid, {
                                    text: `*❪ ALL DOWNLOAD LINKS ❫*\n\n🎮 *ROM:* _${romTitle}_\n📊 *Total:* _${validDownloads.length}_\n⚡ _Generating list..._`
                                }, { quoted: downloadMek });

                                let allLinksText = `🎮 *${romTitle}* (All Files)\n╭──────●➤\n`;
                                for (let i = 0; i < validDownloads.length; i++) {
                                    allLinksText += `*File ${i + 1}:* ${validDownloads[i].link}\n\n`;
                                }
                                allLinksText += `╰──────────●➤\n💡 _Copy into IDM/JDownloader_${DEFAULT_FOOTER}`;

                                await socket.sendMessage(chatJid, { text: allLinksText }, { quoted: downloadMek });
                                await socket.sendMessage(chatJid, { react: { text: '✅', key: downloadMek.key } });
                                return;
                            }

                            // ===== Single file =====
                            const choiceNum = parseInt(downloadChoice) - 1;
                            if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= validDownloads.length) {
                                return socket.sendMessage(chatJid, {
                                    text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 - ${validDownloads.length} (or 00/99)_${DEFAULT_FOOTER}`
                                }, { quoted: downloadMek });
                            }

                            const selectedDownload = validDownloads[choiceNum];
                            await socket.sendMessage(chatJid, { react: { text: '⏳', key: downloadMek.key } });

                            let fileName = `${romTitle.replace(/[^a-zA-Z0-9 ]/g, '').trim()}_File_${choiceNum + 1}.zip`;
                            try {
                                const urlObj = new URL(selectedDownload.link);
                                let lastPart = urlObj.pathname.substring(urlObj.pathname.lastIndexOf('/') + 1);
                                lastPart = decodeURIComponent(lastPart.split('?')[0]);
                                if (lastPart && lastPart.includes('.')) fileName = lastPart;
                            } catch (e) {}

                            try {
                                await socket.sendMessage(chatJid, {
                                    document: { url: selectedDownload.link },
                                    mimetype: 'application/octet-stream',
                                    fileName: fileName,
                                    caption: `🎮 *${romTitle}*\n📌 *File:* ${fileName}\n📦 *Size:* ${selectedDownload.size}${DEFAULT_FOOTER}`
                                }, { quoted: downloadMek });

                                await socket.sendMessage(chatJid, { react: { text: '✅', key: downloadMek.key } });
                            } catch (sendDocErr) {
                                await socket.sendMessage(chatJid, {
                                    text: `🎮 *${romTitle}*\n📌 *File:* ${fileName}\n\n🔗 *Direct Link:*\n${selectedDownload.link}\n\n💡 _Use IDM for full speed_${DEFAULT_FOOTER}`
                                }, { quoted: downloadMek });
                                await socket.sendMessage(chatJid, { react: { text: '🔗', key: downloadMek.key } });
                            }
                        }
                    };

                    const dlCleanupTimeout = setTimeout(() => {
                        global.__hexromListeners.delete(optionsMsgID);
                        console.log(`[HexRom] Cleaned stale download listener: ${optionsMsgID}`);
                    }, 300000);

                    global.__hexromListeners.set(optionsMsgID, { handler: handleDownloadEvent });

                } catch (detailsError) {
                    console.error('Details error:', detailsError);
                    await socket.sendMessage(chatJid, {
                        text: `*❪ ERROR ❫*\n\n❌ *ROM Details Error!*\n🚫 _${detailsError.message}_${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                }
            }
        };

        const cleanupTimeout = setTimeout(() => {
            global.__hexromListeners.delete(messageID);
            console.log(`[HexRom] Cleaned stale selection listener: ${messageID}`);
        }, 180000);

        global.__hexromListeners.set(messageID, { handler: handleSelection });

    } catch (error) {
        console.error('HexRom command error:', error);
        await socket.sendMessage(chatJid, {
            text: `*❪ SYSTEM ERROR ❫*\n\n❌ *System Error!*\n🚫 _${error.message || 'Unknown error'}_\n\n🔄 _Please try again later..._${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }

    break;
}
case 'cartoon2':
case 'sinhalacartoon': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර කාටූනයේ නම ලබාදෙන්න! උදා: .cartoon Ben 10*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const cartoonQuery = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/movies/cartoons';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    let cartoonSelectionListener = null;
    let cartoonEpisodeListener = null;
    let cartoonMasterTimeout = null;

    const clearAllCartoonListeners = () => {
        if (cartoonSelectionListener) {
            socket.ev.off('messages.upsert', cartoonSelectionListener);
            cartoonSelectionListener = null;
        }
        if (cartoonEpisodeListener) {
            socket.ev.off('messages.upsert', cartoonEpisodeListener);
            cartoonEpisodeListener = null;
        }
        if (cartoonMasterTimeout) {
            clearTimeout(cartoonMasterTimeout);
            cartoonMasterTimeout = null;
        }
    };

    const cleanCartoonTitle = (t = '') =>
        t.replace(/\s*\|\s*සිංහල හඩකැවූ.*$/i, '')
         .replace(/\s*Sinhala Dubbed.*$/i, '')
         .trim();

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching cartoons on Cartoons.lk...' }, { quoted: msg });

        // ═══════════════════════════════════════
        // STEP 1 : SEARCH API
        // ═══════════════════════════════════════
        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: cartoonQuery, api_key: API_KEY },
            timeout: 120000
        });

        const searchData = searchRes.data;
        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*කිසිදු කාටූනයක් හමු නොවීය!*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const cartoonList = searchData.data.slice(0, 20);
        let listText = `🧸 *𝗦𝗜𝗡𝗛𝗔𝗟𝗔 𝗖𝗔𝗥𝗧𝗢𝗢𝗡 𝗦𝗘𝗔𝗥𝗖𝗛 : _${cartoonQuery}_*\n╭──────●➤\n*🔢 ʀᴇ𝗽𝗹ʏ ʙᴇʟ𝗼𝘄 ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        cartoonList.forEach((item, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${cleanCartoonTitle(item.title)}*\n    ↳ (${item.quality || 'HD'} | ⭐ ${item.rating || 'N/A'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: cartoonList[0].image || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        cartoonMasterTimeout = setTimeout(() => {
            clearAllCartoonListeners();
        }, 120000);

        // ═══════════════════════════════════════
        // STEP 2 : USER PICKS A CARTOON
        // ═══════════════════════════════════════
        const handleCartoonSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;

            if (isReply) {
                const choice = parseInt(text) - 1;
                if (isNaN(choice) || choice < 0 || choice >= cartoonList.length) {
                    await socket.sendMessage(sender, {
                        text: `❌ කරුණාකර 1 - ${cartoonList.length} අතර අංකයක් ලබාදෙන්න!`
                    }, { quoted: replyMek });
                    return;
                }

                if (cartoonSelectionListener) {
                    socket.ev.off('messages.upsert', cartoonSelectionListener);
                    cartoonSelectionListener = null;
                }

                const chosenCartoon = cartoonList[choice];
                await socket.sendMessage(sender, { text: '⏳ Fetching cartoon details & episodes...' }, { quoted: replyMek });

                try {
                    // ═══════════════════════════════════
                    // STEP 2b : INFO + DL API
                    // ═══════════════════════════════════
                    const infoRes = await axios.get(`${API_BASE}/infodl`, {
                        params: { q: chosenCartoon.link, api_key: API_KEY },
                        timeout: 120000
                    });

                    const cartoonData = infoRes.data?.data;
                    const allDownloads = cartoonData?.downloads || [];

                    if (!cartoonData || allDownloads.length === 0) {
                        throw new Error('බාගත කිරීමේ links හෝ episodes හමු නොවීය.');
                    }

                    // R2 links prioritize කරනවා (direct .mkv / .mp4)
                    const directDownloads = allDownloads.filter(d =>
                        d.link?.includes('r2.cloudflarestorage.com') ||
                        d.link?.endsWith('.mp4') ||
                        d.link?.endsWith('.mkv')
                    );
                    const finalDownloads = directDownloads.length > 0 ? directDownloads : allDownloads;

                    let infoText = `🍀 *${cleanCartoonTitle(cartoonData.title)}*\n\n`;
                    infoText += `📅 *Year:* ${cartoonData.year || 'N/A'}\n`;
                    infoText += `⭐ *IMDb:* ${cartoonData.imdb || 'N/A'}\n`;
                    infoText += `🗣️ *Language:* ${cartoonData.language || 'Sinhala Dubbed'}\n`;
                    infoText += `🎭 *Genres:* ${cartoonData.genres?.join(', ') || 'Cartoon'}\n\n`;
                    infoText += `*Available Episodes / Links:*\n`;

                    finalDownloads.forEach((dl, i) => {
                        infoText += `*${i + 1}.* ${dl.name}\n`;
                    });
                    infoText += `\n👉 *බාගත කිරීමට අදාළ Episode අංකය Reply කරන්න.*`;

                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: cartoonData.image || chosenCartoon.image },
                        caption: infoText
                    }, { quoted: replyMek });

                    const infoMsgID = infoMsg.key.id;

                    // ═══════════════════════════════════
                    // STEP 3 : USER PICKS AN EPISODE
                    // ═══════════════════════════════════
                    const handleEpisodeSelection = async ({ messages: epMessages }) => {
                        const epMek = epMessages?.[0];
                        if (!epMek?.message || epMek.key.remoteJid !== sender) return;

                        const epChoiceText = (epMek.message.conversation || epMek.message.extendedTextMessage?.text || '').trim();
                        const isEpReply = epMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;

                        if (isEpReply) {
                            const epIdx = parseInt(epChoiceText) - 1;
                            if (isNaN(epIdx) || epIdx < 0 || epIdx >= finalDownloads.length) {
                                await socket.sendMessage(sender, {
                                    text: `❌ කරුණාකර 1 - ${finalDownloads.length} අතර Episode අංකයක් ලබාදෙන්න!`
                                }, { quoted: epMek });
                                return;
                            }

                            clearAllCartoonListeners();
                            const selectedEpisode = finalDownloads[epIdx];

                            await socket.sendMessage(sender, { react: { text: '📥', key: epMek.key } });

                            await socket.sendMessage(sender, {
                                text: `⏳ *Downloading Episode:* ${selectedEpisode.name}\n_කරුණාකර ටික වේලාවක් රැඳී සිටින්න, වීඩියෝව ඩවුන්ලෝඩ් වෙමින් පවතී..._`
                            }, { quoted: epMek });

                            try {
                                // Direct Link එක Document MP4 / MKV එකක් ලෙස යැවීම
                                await socket.sendMessage(sender, {
                                    document: { url: selectedEpisode.link },
                                    mimetype: 'video/mp4',
                                    fileName: `${cleanCartoonTitle(cartoonData.title)} - ${selectedEpisode.name.replace(/[📥\[\]]/g, '').trim()}.mp4`,
                                    caption: `✅ *CARTOON DOWNLOADED*\n\n🎬 *Series:* ${cleanCartoonTitle(cartoonData.title)}\n📌 *Episode:* ${selectedEpisode.name}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                }, { quoted: epMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: epMek.key } });
                            } catch (uploadErr) {
                                await socket.sendMessage(sender, {
                                    text: `❌ වීඩියෝව යැවීමේදී දෝෂයක් ඇති විය: ${uploadErr.message}\n\n🔗 Direct Link එක: ${selectedEpisode.link}`
                                }, { quoted: epMek });
                            }
                        }
                    };

                    cartoonEpisodeListener = handleEpisodeSelection;
                    socket.ev.on('messages.upsert', handleEpisodeSelection);

                } catch (infoErr) {
                    clearAllCartoonListeners();
                    await socket.sendMessage(sender, { text: `❌ Cartoon Info Error: ${infoErr.message}` }, { quoted: replyMek });
                }
            }
        };

        cartoonSelectionListener = handleCartoonSelection;
        socket.ev.on('messages.upsert', handleCartoonSelection);

    } catch (err) {
        clearAllCartoonListeners();
        await socket.sendMessage(sender, {
            text: `❌ Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
}
// ==========================================
// CHITHRAPATA - Movie & TV Series Downloader
// ==========================================
case 'chithrapata':
case 'chithra':
case 'chmovie': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .chithrapata Vivaah*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const chithraQuery = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/chithrapata';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    let chithraSelectionListener = null;
    let chithraDownloadListener = null;
    let chithraMasterTimeout = null;

    const clearAllChithraListeners = () => {
        if (chithraSelectionListener) { socket.ev.off('messages.upsert', chithraSelectionListener); chithraSelectionListener = null; }
        if (chithraDownloadListener)  { socket.ev.off('messages.upsert', chithraDownloadListener);  chithraDownloadListener  = null; }
        if (chithraMasterTimeout)     { clearTimeout(chithraMasterTimeout); chithraMasterTimeout = null; }
    };

    const cleanChithraTitle = (t = '') =>
        t.replace(/\s*\|\s*සිංහල උපසිරැසි.*$/i, '')
         .replace(/\s*Sinhala Subtitles.*$/i, '')
         .trim();

    const parseSizeMB = (sizeStr) => {
        if (!sizeStr) return 0;
        const m = sizeStr.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching Chithrapata.com...' }, { quoted: msg });

        // ═══ STEP 1 : SEARCH (Movies + TV) ═══
        const [moviesRes, tvRes] = await Promise.all([
            axios.get(`${API_BASE}/search`, { params: { q: chithraQuery, api_key: API_KEY }, timeout: 120000 }).catch(() => ({ data: { results: [] } })),
            axios.get(`https://api.chamindu.site/api/v1/tv/chithrapata/search`, { params: { q: chithraQuery, api_key: API_KEY }, timeout: 120000 }).catch(() => ({ data: { results: [] } }))
        ]);

        const movieResults = (moviesRes.data?.results || []).map(r => ({ ...r, _type: 'movie' }));
        const tvResults = (tvRes.data?.results || []).map(r => ({ ...r, _type: 'tv' }));
        const allResults = [...movieResults, ...tvResults];

        if (allResults.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage('❌ NO RESULTS', '*කිසිදු චිත්‍රපටයක්/සිරිසක් හමු නොවීය!*', `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
            }, { quoted: msg });
            break;
        }

        const list = allResults.slice(0, 20);
        let listText = `🎬 *𝗖𝗛𝗜𝗧𝗛𝗥𝗔𝗣𝗔𝗧𝗔 𝗦𝗘𝗔𝗥𝗖𝗛 : _${chithraQuery}_*\n╭──────●➤\n*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        list.forEach((item, i) => {
            const typeIcon = item._type === 'tv' ? '📺' : '🎥';
            listText += `*${typeIcon} ${i + 1} ┃❭❭ ${cleanChithraTitle(item.title)}*\n    ↳ (${item.year || 'N/A'} | ⭐ ${item.rating || 'N/A'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: list[0].thumbnail || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;
        chithraMasterTimeout = setTimeout(clearAllChithraListeners, 120000);

        // ═══ STEP 2 : USER PICKS ═══
        const handleChithraSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            if (replyMek.message.extendedTextMessage?.contextInfo?.stanzaId !== searchMsgID) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= list.length) {
                return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${list.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: replyMek });
            }

            if (chithraSelectionListener) { socket.ev.off('messages.upsert', chithraSelectionListener); chithraSelectionListener = null; }

            const chosen = list[choice];
            await socket.sendMessage(sender, { text: '⏳ Fetching details...' }, { quoted: replyMek });

            try {
                // ─── TV SERIES FLOW ───
                if (chosen._type === 'tv') {
                    const tvInfoRes = await axios.get('https://api.chamindu.site/api/v1/tv/chithrapata/info', {
                        params: { url: chosen.url, api_key: API_KEY },
                        timeout: 120000
                    });

                    const tvData = tvInfoRes.data?.data;
                    if (!tvData) throw new Error('TV Series details හමු නොවීය.');

                    // TV series info message
                    let tvInfoText = `📺 *${cleanChithraTitle(tvData.title)}*\n\n`;
                    tvInfoText += `📅 *Year:* ${tvData.year || 'N/A'}\n`;
                    tvInfoText += `⭐ *Rating:* ${tvData.rating || 'N/A'}\n`;
                    tvInfoText += `🎭 *Genres:* ${tvData.genres?.join(', ') || 'N/A'}\n`;
                    tvInfoText += `📊 *Seasons:* ${tvData.total_seasons || 'N/A'}\n`;
                    tvInfoText += `📊 *Episodes:* ${tvData.total_episodes || 'N/A'}\n\n`;
                    if (tvData.story) tvInfoText += `📖 *Story:*\n_${tvData.story.substring(0, 200)}..._\n`;

                    await socket.sendMessage(sender, {
                        image: { url: tvData.image || chosen.thumbnail },
                        caption: tvInfoText
                    }, { quoted: replyMek });

                    // Season list
                    const seasons = tvData.seasons || [];
                    let seasonsText = `*❪ SEASONS ❫*\n\n`;
                    seasons.forEach((s, i) => {
                        seasonsText += `*${i + 1}.* ${s.season_name} _(${s.total_episodes} eps)_\n`;
                    });
                    seasonsText += `\n👉 *Season එකක් Reply කරන්න.*`;

                    const seasonMsg = await socket.sendMessage(sender, { text: seasonsText }, { quoted: replyMek });
                    const seasonMsgID = seasonMsg.key.id;

                    const handleSeasonSelect = async ({ messages: seasonMsgs }) => {
                        const seasonMek = seasonMsgs?.[0];
                        if (!seasonMek?.message || seasonMek.key.remoteJid !== sender) return;

                        const seasonText = (seasonMek.message.conversation || seasonMek.message.extendedTextMessage?.text || '').trim();
                        if (seasonMek.message.extendedTextMessage?.contextInfo?.stanzaId !== seasonMsgID) return;

                        const sIdx = parseInt(seasonText) - 1;
                        if (isNaN(sIdx) || sIdx < 0 || sIdx >= seasons.length) {
                            return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${seasons.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: seasonMek });
                        }

                        socket.ev.off('messages.upsert', handleSeasonSelect);
                        const selectedSeason = seasons[sIdx];

                        let epText = `📺 *${selectedSeason.season_name}*\n\n`;
                        selectedSeason.episodes.forEach((ep, i) => {
                            epText += `*${i + 1}.* E${ep.episode} - ${ep.title}\n    ↳ _${ep.date || 'N/A'}_\n`;
                        });
                        epText += `\n👉 *Episode අංකය Reply කරන්න.*`;

                        const epMsg = await socket.sendMessage(sender, { text: epText }, { quoted: seasonMek });
                        const epMsgID = epMsg.key.id;

                        const handleEpisodeSelect = async ({ messages: epMsgs }) => {
                            const epMek = epMsgs?.[0];
                            if (!epMek?.message || epMek.key.remoteJid !== sender) return;

                            const epText = (epMek.message.conversation || epMek.message.extendedTextMessage?.text || '').trim();
                            if (epMek.message.extendedTextMessage?.contextInfo?.stanzaId !== epMsgID) return;

                            const eIdx = parseInt(epText) - 1;
                            if (isNaN(eIdx) || eIdx < 0 || eIdx >= selectedSeason.episodes.length) {
                                return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${selectedSeason.episodes.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: epMek });
                            }

                            socket.ev.off('messages.upsert', handleEpisodeSelect);
                            const selectedEp = selectedSeason.episodes[eIdx];

                            await socket.sendMessage(sender, {
                                text: `⏳ *Fetching Episode Links:* E${selectedEp.episode}\n_${selectedEp.title}_`
                            }, { quoted: epMek });

                            try {
                                const epInfoRes = await axios.get('https://api.chamindu.site/api/v1/tv/chithrapata/episode', {
                                    params: { url: selectedEp.url, api_key: API_KEY },
                                    timeout: 120000
                                });

                                const epData = epInfoRes.data?.data || epInfoRes.data;
                                const epDownloads = epData?.downloads || [];

                                if (epDownloads.length === 0) {
                                    throw new Error('Download links හමු නොවීය.');
                                }

                                // Pixeldrain links විතරක් filter කරන්න
                                const pixeldrainLinks = epDownloads.filter(d =>
                                    d.link?.includes('pixeldrain.com') || d.direct_link?.includes('pixeldrain.com')
                                );
                                const finalDls = pixeldrainLinks.length > 0 ? pixeldrainLinks : epDownloads;

                                let dlText = `📥 *Episode ${selectedEp.episode}: ${selectedEp.title}*\n\n`;
                                finalDls.forEach((dl, i) => {
                                    dlText += `*${i + 1}.* ${dl.quality || dl.name} _(${dl.size || 'N/A'})_\n`;
                                });
                                dlText += `\n👉 *Quality අංකය Reply කරන්න.*`;

                                const dlMsg = await socket.sendMessage(sender, { text: dlText }, { quoted: epMek });
                                const dlMsgID = dlMsg.key.id;

                                const handleEpDownload = async ({ messages: dlMsgs }) => {
                                    const dlMek = dlMsgs?.[0];
                                    if (!dlMek?.message || dlMek.key.remoteJid !== sender) return;

                                    const dlText2 = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                                    if (dlMek.message.extendedTextMessage?.contextInfo?.stanzaId !== dlMsgID) return;

                                    const dIdx = parseInt(dlText2) - 1;
                                    if (isNaN(dIdx) || dIdx < 0 || dIdx >= finalDls.length) {
                                        return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${finalDls.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: dlMek });
                                    }

                                    socket.ev.off('messages.upsert', handleEpDownload);
                                    const sel = finalDls[dIdx];
                                    const dlUrl = sel.direct_link || sel.link;

                                    await socket.sendMessage(sender, { react: { text: '📥', key: dlMek.key } });
                                    await socket.sendMessage(sender, {
                                        text: `⏳ *Sending:* ${sel.quality || sel.name}\n📦 *Size:* ${sel.size || 'N/A'}\n_කරුණාකර රැඳී සිටින්න..._`
                                    }, { quoted: dlMek });

                                    try {
                                        const fileName = `${cleanChithraTitle(tvData.title)} S${selectedSeason.season}E${selectedEp.episode} - ${selectedEp.title.replace(/[^a-zA-Z0-9 ]/g, '').trim()}.mp4`;

                                        await socket.sendMessage(sender, {
                                            document: { url: dlUrl },
                                            mimetype: 'video/mp4',
                                            fileName: fileName,
                                            caption: `✅ *CHITHRAPATA TV*\n\n📺 *Series:* ${cleanChithraTitle(tvData.title)}\n📀 *Season:* ${selectedSeason.season}\n📌 *Episode:* ${selectedEp.episode}\n🎞 *Quality:* ${sel.quality || 'HD'}\n📦 *Size:* ${sel.size || 'N/A'}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                        }, { quoted: dlMek });

                                        await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });

                                    } catch (sendErr) {
                                        await socket.sendMessage(sender, {
                                            text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${dlUrl}`
                                        }, { quoted: dlMek });
                                    }
                                };

                                socket.ev.on('messages.upsert', handleEpDownload);
                                setTimeout(() => socket.ev.off('messages.upsert', handleEpDownload), 300000);

                            } catch (epErr) {
                                await socket.sendMessage(sender, { text: `❌ Episode Error: ${epErr.message}` }, { quoted: epMek });
                            }
                        };

                        socket.ev.on('messages.upsert', handleEpisodeSelect);
                        setTimeout(() => socket.ev.off('messages.upsert', handleEpisodeSelect), 300000);
                    };

                    socket.ev.on('messages.upsert', handleSeasonSelect);
                    setTimeout(() => socket.ev.off('messages.upsert', handleSeasonSelect), 300000);

                    return;   // TV done
                }

                // ─── MOVIE FLOW ───
                const infoRes = await axios.get(`${API_BASE}/info`, {
                    params: { url: chosen.url, api_key: API_KEY },
                    timeout: 120000
                });

                const movieData = infoRes.data?.result;
                if (!movieData) throw new Error('Movie details හමු නොවීය.');

                const movieTitle = cleanChithraTitle(movieData.title);
                const allDownloads = movieData.downloads || [];

                // Pixeldrain links විතරක් prioritize කරන්න
                const pixeldrainLinks = allDownloads.filter(d =>
                    d.link?.includes('pixeldrain.com') || d.direct_link?.includes('pixeldrain.com')
                );
                const finalDls = pixeldrainLinks.length > 0 ? pixeldrainLinks : allDownloads;

                let infoText = `🎬 *${movieTitle}*\n\n`;
                infoText += `📅 *Year:* ${movieData.year || 'N/A'}\n`;
                infoText += `⭐ *Rating:* ${movieData.rating || 'N/A'}\n`;
                infoText += `🌍 *Country:* ${movieData.country || 'N/A'}\n`;
                infoText += `🎭 *Genres:* ${movieData.genres?.join(', ') || 'N/A'}\n\n`;
                if (movieData.story) infoText += `📖 *Story:*\n_${movieData.story.substring(0, 200)}..._\n\n`;

                infoText += `*Available Downloads:*\n`;
                finalDls.forEach((dl, i) => {
                    const sizeMB = parseSizeMB(dl.size);
                    let note = '';
                    if (sizeMB > 2000) note = ' ⚠️';
                    else if (sizeMB > 0) note = ' ✓';
                    infoText += `*${i + 1}.* ${dl.quality} _(${dl.size || 'N/A'})_${note}\n`;
                });
                infoText += `\n👉 *බාගත කිරීමට අදාළ අංකය Reply කරන්න.*`;

                const infoMsg = await socket.sendMessage(sender, {
                    image: { url: movieData.image || chosen.thumbnail },
                    caption: infoText
                }, { quoted: replyMek });

                const infoMsgID = infoMsg.key.id;

                const handleChithraDownload = async ({ messages: dlMessages }) => {
                    const dlMek = dlMessages?.[0];
                    if (!dlMek?.message || dlMek.key.remoteJid !== sender) return;

                    const dlChoiceText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                    if (dlMek.message.extendedTextMessage?.contextInfo?.stanzaId !== infoMsgID) return;

                    const dlIdx = parseInt(dlChoiceText) - 1;
                    if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= finalDls.length) {
                        return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${finalDls.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: dlMek });
                    }

                    clearAllChithraListeners();
                    const selectedDl = finalDls[dlIdx];
                    const dlUrl = selectedDl.direct_link || selectedDl.link;
                    const sizeMB = parseSizeMB(selectedDl.size);

                    // Telegram link නම් → link only
                    if (dlUrl.includes('t.me/')) {
                        await socket.sendMessage(sender, { react: { text: '🔗', key: dlMek.key } });
                        return socket.sendMessage(sender, {
                            text: `🎬 *${movieTitle}*\n\n📌 *Quality:* ${selectedDl.quality}\n📦 *Size:* ${selectedDl.size || 'N/A'}\n\n📱 *Telegram Link:*\n${dlUrl}\n\n_Telegram bot එකෙන් download කරන්න._\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: dlMek });
                    }

                    // 2GB ට වඩා ලොකු නම් → link only
                    if (sizeMB > 2000) {
                        await socket.sendMessage(sender, { react: { text: '⚠️', key: dlMek.key } });
                        return socket.sendMessage(sender, {
                            text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${movieTitle}*\n📌 *${selectedDl.quality}*\n📦 *${selectedDl.size}*\n\n🔗 *Direct Link:*\n${dlUrl}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: dlMek });
                    }

                    await socket.sendMessage(sender, { react: { text: '📥', key: dlMek.key } });
                    await socket.sendMessage(sender, {
                        text: `⏳ *Downloading:* ${selectedDl.quality}\n📦 *Size:* ${selectedDl.size || 'N/A'}\n_කරුණාකර රැඳී සිටින්න..._`
                    }, { quoted: dlMek });

                    try {
                        const fileName = `${movieTitle.replace(/[^a-zA-Z0-9 ]/g, '').trim()} - ${selectedDl.quality}.mp4`;

                        await socket.sendMessage(sender, {
                            document: { url: dlUrl },
                            mimetype: 'video/mp4',
                            fileName: fileName,
                            caption: `✅ *CHITHRAPATA MOVIE*\n\n🎬 *Title:* ${movieTitle}\n📅 *Year:* ${movieData.year || 'N/A'}\n⭐ *Rating:* ${movieData.rating || 'N/A'}\n📌 *Quality:* ${selectedDl.quality}\n📦 *Size:* ${selectedDl.size || 'N/A'}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: dlMek });

                        await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });

                    } catch (sendErr) {
                        await socket.sendMessage(sender, {
                            text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${dlUrl}`
                        }, { quoted: dlMek });
                    }
                };

                chithraDownloadListener = handleChithraDownload;
                socket.ev.on('messages.upsert', handleChithraDownload);

            } catch (infoErr) {
                clearAllChithraListeners();
                await socket.sendMessage(sender, { text: `❌ Chithrapata Info Error: ${infoErr.message}` }, { quoted: replyMek });
            }
        };

        chithraSelectionListener = handleChithraSelection;
        socket.ev.on('messages.upsert', handleChithraSelection);

    } catch (err) {
        clearAllChithraListeners();
        await socket.sendMessage(sender, {
            text: `❌ Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
}

case 'jid':
case 'getjid': {
    const chatJid = msg.key.remoteJid;
    const senderJid = msg.key.participant || msg.key.remoteJid;
    const isGroup = chatJid.endsWith('@g.us');

    let jidText = `📌 *JID INFORMATION*\n\n`;
    jidText += `📍 *Chat / Remote JID:* \n\`${chatJid}\`\n\n`;
    
    if (isGroup) {
        jidText += `👤 *Sender JID:* \n\`${senderJid}\`\n\n`;
        jidText += `🏠 *Type:* Group Chat\n`;
    } else {
        jidText += `🏠 *Type:* Private Chat (DM)\n`;
    }

    await socket.sendMessage(sender, {
        text: jidText
    }, { quoted: msg });
    
    break;
}

case 'dubzone':
case 'dubzonesearch': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර සෙවිය යුතු DubZone චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .dubzone The Lorax*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const dubQuery = args.join(' ');
    const API_BASE = 'https://api-siteh-22e22e4cb068.herokuapp.com/api/dubzone';

    let dubSelectionListener = null;
    let dubDownloadListener = null;
    let dubMasterTimeout = null;

    const clearAllDubListeners = () => {
        if (dubSelectionListener) {
            socket.ev.off('messages.upsert', dubSelectionListener);
            dubSelectionListener = null;
        }
        if (dubDownloadListener) {
            socket.ev.off('messages.upsert', dubDownloadListener);
            dubDownloadListener = null;
        }
        if (dubMasterTimeout) {
            clearTimeout(dubMasterTimeout);
            dubMasterTimeout = null;
        }
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching movies on DubZoneLK...' }, { quoted: msg });

        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { query: dubQuery },
            timeout: 120000
        });

        const searchData = searchRes.data;
        if (!searchData.success || !searchData.results || searchData.results.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*කිසිදු චිත්‍රපටයක් හමු නොවීය!*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const movieList = searchData.results.slice(0, 10);
        let listText = `🎬 *𝗗𝗨𝗕𝗭𝗢𝗡𝗘 𝗦𝗘𝗔𝗥𝗖𝗛 : _${dubQuery}_*\n╭──────●➤\n*🔢 ʀᴇ𝗽𝗹ʏ ʙᴇ𝗹𝗼𝘄 ɴᴜᴍ𝗯𝗲𝗿*\n╰──────────●➤\n╭──────●➤\n`;

        movieList.forEach((item, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${item.title}*\n    ↳ (📅 ${item.date || 'N/A'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: movieList[0].thumbnail || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        dubMasterTimeout = setTimeout(() => {
            clearAllDubListeners();
        }, 120000);

        const handleMovieSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;

            if (isReply) {
                const choice = parseInt(text) - 1;
                if (isNaN(choice) || choice < 0 || choice >= movieList.length) {
                    await socket.sendMessage(sender, {
                        text: `❌ කරුණාකර 1 - ${movieList.length} අතර අංකයක් ලබාදෙන්න!`
                    }, { quoted: replyMek });
                    return;
                }

                if (dubSelectionListener) {
                    socket.ev.off('messages.upsert', dubSelectionListener);
                    dubSelectionListener = null;
                }

                const chosenMovie = movieList[choice];
                await socket.sendMessage(sender, { text: '⏳ Fetching download qualities...' }, { quoted: replyMek });

                try {
                    const downloadsRes = await axios.get(`${API_BASE}/downloads`, {
                        params: { slug: chosenMovie.slug },
                        timeout: 120000
                    });

                    const dlData = downloadsRes.data;
                    const downloadQualities = dlData?.downloads || [];

                    if (!dlData.success || downloadQualities.length === 0) {
                        throw new Error('ඩවුන්ලෝඩ් ලින්ක්ස් හමු නොවීය.');
                    }

                    let infoText = `📥 *${dlData.title || chosenMovie.title}*\n\n`;
                    infoText += `*Available Qualities & Sizes:* \n`;

                    let flatLinks = [];
                    let count = 1;

                    downloadQualities.forEach((qual) => {
                        qual.links.forEach((linkObj) => {
                            flatLinks.push({
                                quality: qual.quality,
                                size: qual.size,
                                provider: linkObj.provider,
                                url: linkObj.url
                            });
                            infoText += `*${count}.* [${qual.quality}] Size: ${qual.size} (${linkObj.provider})\n`;
                            count++;
                        });
                    });

                    infoText += `\n👉 *බාගත කිරීමට අවශ්‍ය අංකය Reply කරන්න.*`;

                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: chosenMovie.thumbnail },
                        caption: infoText
                    }, { quoted: replyMek });

                    const infoMsgID = infoMsg.key.id;

                    const handleDownloadSelection = async ({ messages: dlMessages }) => {
                        const dlMek = dlMessages?.[0];
                        if (!dlMek?.message || dlMek.key.remoteJid !== sender) return;

                        const dlChoiceText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                        const isDlReply = dlMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;

                        if (isDlReply) {
                            const dlIdx = parseInt(dlChoiceText) - 1;
                            if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= flatLinks.length) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ කරුණාකර 1 - ${flatLinks.length} අතර අංකයක් ලබාදෙන්න!` 
                                }, { quoted: dlMek });
                                return;
                            }

                            clearAllDubListeners();
                            const selectedDL = flatLinks[dlIdx];

                            await socket.sendMessage(sender, { react: { text: '📥', key: dlMek.key } });

                            await socket.sendMessage(sender, { 
                                text: `⏳ *Downloading Movie (${selectedDL.quality} - ${selectedDL.size})...\n_ගොනුවේ ප්‍රමාණය මත ටික වේලාවක් ගත විය හැක..._` 
                            }, { quoted: dlMek });

                            try {
                                await socket.sendMessage(sender, {
                                    document: { url: selectedDL.url },
                                    mimetype: 'video/mp4',
                                    fileName: `${chosenMovie.title.replace(/[^a-zA-Z0-9]/g, '_')}_${selectedDL.quality}.mp4`,
                                    caption: `✅ *MOVIE DOWNLOADED*\n\n🎬 *Title:* ${chosenMovie.title}\n📌 *Quality:* ${selectedDL.quality} (${selectedDL.size})\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                }, { quoted: dlMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });
                            } catch (uploadErr) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ මූවි එක යැවීමේදී දෝෂයක් ඇති විය: ${uploadErr.message}\n\n🔗 Direct Link එක: ${selectedDL.url}` 
                                }, { quoted: dlMek });
                            }
                        }
                    };

                    dubDownloadListener = handleDownloadSelection;
                    socket.ev.on('messages.upsert', handleDownloadSelection);

                } catch (infoErr) {
                    clearAllDubListeners();
                    await socket.sendMessage(sender, { text: `❌ DubZone Info Error: ${infoErr.message}` }, { quoted: replyMek });
                }
            }
        };

        dubSelectionListener = handleMovieSelection;
        socket.ev.on('messages.upsert', handleMovieSelection);

    } catch (err) {
        clearAllDubListeners();
        await socket.sendMessage(sender, {
            text: `❌ Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
}   

    case 'thinkiri':
case 'thenkiri': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර සෙවිය යුතු Movie එකේ හෝ TV Series එකේ නම ලබාදෙන්න! උදා: .thinkiri newborn*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const thinkiriQuery = args.join(' ');
    const API_BASE = 'https://api-siteh-22e22e4cb068.herokuapp.com/tinkiri';
    const API_KEY = 'lakiya_2f3b6c382d1236ad7a08d56331fb679935d51dfc846df2c254093fd1fff9494e';

    let thinkiriSelectionListener = null;
    let thinkiriMasterTimeout = null;

    const clearAllThinkiriListeners = () => {
        if (thinkiriSelectionListener) {
            socket.ev.off('messages.upsert', thinkiriSelectionListener);
            thinkiriSelectionListener = null;
        }
        if (thinkiriMasterTimeout) {
            clearTimeout(thinkiriMasterTimeout);
            thinkiriMasterTimeout = null;
        }
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching movies on TheNkiri...' }, { quoted: msg });

        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { query: thinkiriQuery, api_key: API_KEY },
            timeout: 120000
        });

        const searchData = searchRes.data;
        if (!searchData.status || !searchData.data || !searchData.data.results || searchData.data.results.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*කිසිදු ප්‍රතිඵලයක් හමු නොවීය!*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        // Duplicate results ඉවත් කර ගැනීමට (Unique URL මත පදනම්ව)
        const rawResults = searchData.data.results;
        const uniqueResults = Array.from(new Map(rawResults.map(item => [item.url, item])).values());
        const movieList = uniqueResults.slice(0, 10);

        let listText = `🎬 *𝗧𝗛𝗘𝗡𝗞𝗜𝗥𝗜 𝗦𝗘𝗔𝗥𝗖𝗛 : _${thinkiriQuery}_*\n╭──────●➤\n*🔢 ʀᴇ𝗽𝗹ʏ ʙᴇ𝗹𝗼𝘄 ɴᴜᴍ𝗯𝗲𝗿*\n╰──────────●➤\n╭──────●➤\n`;

        movieList.forEach((item, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${item.title}*\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: movieList[0].thumbnail || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        thinkiriMasterTimeout = setTimeout(() => {
            clearAllThinkiriListeners();
        }, 120000);

        const handleMovieSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;

            if (isReply) {
                const choice = parseInt(text) - 1;
                if (isNaN(choice) || choice < 0 || choice >= movieList.length) {
                    await socket.sendMessage(sender, {
                        text: `❌ කරුණාකර 1 - ${movieList.length} අතර අංකයක් ලබාදෙන්න!`
                    }, { quoted: replyMek });
                    return;
                }

                clearAllThinkiriListeners();
                const chosenMovie = movieList[choice];

                await socket.sendMessage(sender, { react: { text: '📥', key: replyMek.key } });
                await socket.sendMessage(sender, { text: '⏳ Fetching download links & details...' }, { quoted: replyMek });

                try {
                    const detailsRes = await axios.get(`${API_BASE}/details`, {
                        params: { url: chosenMovie.url, api_key: API_KEY },
                        timeout: 120000
                    });

                    const detailsData = detailsRes.data?.data;
                    const movieInfo = detailsData?.movie;
                    const downloadOptions = detailsData?.download_options || [];

                    if (!detailsData || downloadOptions.length === 0) {
                        throw new Error('බාගත කිරීමේ links හමු නොවීය.');
                    }

                    const directDownloadUrl = downloadOptions[0].direct_download_url;
                    const fileSize = downloadOptions[0].file_size || 'Unknown';
                    const fileName = downloadOptions[0].file_name || movieInfo?.title || 'movie.mkv';

                    let infoText = `📥 *${movieInfo?.title || chosenMovie.title}*\n\n`;
                    infoText += `📦 *File Size:* ${fileSize}\n`;
                    infoText += `📂 *Status:* ${downloadOptions[0].status || 'Success'}\n\n`;
                    infoText += `_වීඩියෝව හෝ ගොනුව ඩවුන්ලෝඩ් වෙමින් පවතී..._`;

                    await socket.sendMessage(sender, {
                        document: { url: directDownloadUrl },
                        mimetype: 'video/mp4',
                        fileName: fileName,
                        caption: infoText
                    }, { quoted: replyMek });

                    await socket.sendMessage(sender, { react: { text: '✅', key: replyMek.key } });

                } catch (detailsErr) {
                    await socket.sendMessage(sender, { 
                        text: `❌ Details Error: ${detailsErr.message}` 
                    }, { quoted: replyMek });
                }
            }
        };

        thinkiriSelectionListener = handleMovieSelection;
        socket.ev.on('messages.upsert', handleMovieSelection);

    } catch (err) {
        clearAllThinkiriListeners();
        await socket.sendMessage(sender, {
            text: `❌ Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
}
case 'sinhalatop':
case 'sinhalatopsearch': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර සෙවිය යුතු චිත්‍රපටයේ හෝ කතාමාලාවේ නම ලබාදෙන්න! උදා: .sinhalatop Alpha*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const sinhalaTopQuery = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/cartoons/sinhalatop';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    let sinhalaTopSelectionListener = null;
    let sinhalaTopDownloadListener = null;
    let sinhalaTopMasterTimeout = null;

    const clearAllSinhalaTopListeners = () => {
        if (sinhalaTopSelectionListener) {
            socket.ev.off('messages.upsert', sinhalaTopSelectionListener);
            sinhalaTopSelectionListener = null;
        }
        if (sinhalaTopDownloadListener) {
            socket.ev.off('messages.upsert', sinhalaTopDownloadListener);
            sinhalaTopDownloadListener = null;
        }
        if (sinhalaTopMasterTimeout) {
            clearTimeout(sinhalaTopMasterTimeout);
            sinhalaTopMasterTimeout = null;
        }
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching movies on SinhalaTop...' }, { quoted: msg });

        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: sinhalaTopQuery, api_key: API_KEY },
            timeout: 120000
        });

        const searchData = searchRes.data;
        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*කිසිදු ප්‍රතිඵලයක් හමු නොවීය!*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const movieList = searchData.data.slice(0, 10);
        let listText = `🎬 *𝗦𝗜𝗡𝗛𝗔𝗟𝗔.𝗧𝗢𝗣 𝗦𝗘𝗔𝗥𝗖𝗛 : _${sinhalaTopQuery}_*\n╭──────●➤\n*🔢 ʀᴇ𝗽𝗹ʏ ʙᴇ𝗹𝗼𝘄 ɴᴜᴍ𝗯𝗲𝗿*\n╰──────────●➤\n╭──────●➤\n`;

        movieList.forEach((item, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${item.title}*\n    ↳ (${item.type || 'Movie'} | ⭐ ${item.rating || 'N/A'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: movieList[0].image || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        sinhalaTopMasterTimeout = setTimeout(() => {
            clearAllSinhalaTopListeners();
        }, 120000);

        const handleMovieSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;

            if (isReply) {
                const choice = parseInt(text) - 1;
                if (isNaN(choice) || choice < 0 || choice >= movieList.length) {
                    await socket.sendMessage(sender, {
                        text: `❌ කරුණාකර 1 - ${movieList.length} අතර අංකයක් ලබාදෙන්න!`
                    }, { quoted: replyMek });
                    return;
                }

                if (sinhalaTopSelectionListener) {
                    socket.ev.off('messages.upsert', sinhalaTopSelectionListener);
                    sinhalaTopSelectionListener = null;
                }

                const chosenMovie = movieList[choice];
                await socket.sendMessage(sender, { text: '⏳ Fetching movie info & subtitle links...' }, { quoted: replyMek });

                try {
                    const infoRes = await axios.get(`${API_BASE}/infodl`, {
                        params: { q: chosenMovie.link, api_key: API_KEY },
                        timeout: 120000
                    });

                    const movieData = infoRes.data?.data;
                    const allDownloads = movieData?.downloads || [];

                    if (!movieData || allDownloads.length === 0) {
                        throw new Error('උපසිරැසි හෝ ඩවුන්ලෝඩ් ලින්ක්ස් හමු නොවීය.');
                    }

                    let infoText = `🍀 *${movieData.title}*\n\n`;
                    if (movieData.imdb) infoText += `⭐ *IMDb:* ${movieData.imdb}\n`;
                    if (movieData.language) infoText += `🗣️ *Language:* ${movieData.language}\n`;
                    if (movieData.genres) infoText += `🎭 *Genres:* ${movieData.genres.join(', ')}\n\n`;

                    if (movieData.story) {
                        infoText += `📖 *Story:* ${movieData.story.substring(0, 300)}...\n\n`;
                    }

                    infoText += `*Available Subtitle / Download Files:*\n`;
                    allDownloads.forEach((dl, i) => {
                        infoText += `*${i + 1}.* ${dl.name}\n`;
                    });
                    infoText += `\n👉 *බාගත කිරීමට අදාළ අංකය Reply කරන්න.*`;

                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: movieData.image || chosenMovie.image },
                        caption: infoText
                    }, { quoted: replyMek });

                    const infoMsgID = infoMsg.key.id;

                    const handleDownloadSelection = async ({ messages: dlMessages }) => {
                        const dlMek = dlMessages?.[0];
                        if (!dlMek?.message || dlMek.key.remoteJid !== sender) return;

                        const dlChoiceText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                        const isDlReply = dlMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;

                        if (isDlReply) {
                            const dlIdx = parseInt(dlChoiceText) - 1;
                            if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= allDownloads.length) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ කරුණාකර 1 - ${allDownloads.length} අතර අංකයක් ලබාදෙන්න!` 
                                }, { quoted: dlMek });
                                return;
                            }

                            clearAllSinhalaTopListeners();
                            const selectedDownload = allDownloads[dlIdx];

                            await socket.sendMessage(sender, { react: { text: '📥', key: dlMek.key } });

                            await socket.sendMessage(sender, { 
                                text: `⏳ *Downloading File:* ${selectedDownload.name}\n_කරුණාකර ටික වේලාවක් රැඳී සිටින්න, ෆိုင် එක සූදානම් වෙමින් පවතී..._` 
                            }, { quoted: dlMek });

                            try {
                                // ZIP හෝ Document එකක් ලෙස යැවීම
                                await socket.sendMessage(sender, {
                                    document: { url: selectedDownload.link },
                                    mimetype: 'application/zip',
                                    fileName: `${movieData.title.split(' ')[0]} - Subtitles.zip`,
                                    caption: `✅ *FILE DOWNLOADED*\n\n🎬 *Movie:* ${movieData.title}\n📌 *Source:* ${selectedDownload.name}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                }, { quoted: dlMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });
                            } catch (uploadErr) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ ෆိုင် එක යැවීමේදී දෝෂයක් ඇති විය: ${uploadErr.message}\n\n🔗 Direct Link එක: ${selectedDownload.link}` 
                                }, { quoted: dlMek });
                            }
                        }
                    };

                    sinhalaTopDownloadListener = handleDownloadSelection;
                    socket.ev.on('messages.upsert', handleDownloadSelection);

                } catch (infoErr) {
                    clearAllSinhalaTopListeners();
                    await socket.sendMessage(sender, { text: `❌ SinhalaTop Info Error: ${infoErr.message}` }, { quoted: replyMek });
                }
            }
        };

        sinhalaTopSelectionListener = handleMovieSelection;
        socket.ev.on('messages.upsert', handleMovieSelection);

    } catch (err) {
        clearAllSinhalaTopListeners();
        await socket.sendMessage(sender, {
            text: `❌ Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
}                                    
 case 'statusdl':
case 'sdl': {
    const quoted = msg.message.extendedTextMessage?.contextInfo?.quotedMessage;
    if (!quoted) {
        await socket.sendMessage(sender, { text: '❌ කරුණාකර WhatsApp Status එකකට රිප්ளை කර `.statusdl` ලෙස ලබාදෙන්න!' }, { quoted: msg });
        break;
    }

    try {
        await socket.sendMessage(sender, { react: { text: '📥', key: msg.key } });
        let mediaMessage = quoted.imageMessage || quoted.videoMessage || quoted.audioMessage;

        if (!mediaMessage) {
            await socket.sendMessage(sender, { text: '❌ මෙම මීඩියා වර්ගය ඩවුන්ලෝඩ් කළ නොහැක!' }, { quoted: msg });
            break;
        }

        const type = quoted.imageMessage ? 'image' : quoted.videoMessage ? 'video' : 'audio';
        const stream = await downloadContentFromMessage(mediaMessage, type);
        let buffer = Buffer.from([]);
        for await (const chunk of stream) {
            buffer = Buffer.concat([buffer, chunk]);
        }

        let caption = `📥 *Status Downloaded*\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;
        if (type === 'image') {
            await socket.sendMessage(sender, { image: buffer, caption: caption }, { quoted: msg });
        } else if (type === 'video') {
            await socket.sendMessage(sender, { video: buffer, caption: caption }, { quoted: msg });
        } else if (type === 'audio') {
            await socket.sendMessage(sender, { audio: buffer, mimetype: 'audio/mp4', ptt: false }, { quoted: msg });
        }

        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
    } catch (err) {
        await socket.sendMessage(sender, { text: `❌ Status ඩවුන්ලෝඩ් කිරීමේදී දෝෂයක් ඇති විය: ${err.message}` }, { quoted: msg });
    }
    break;
}
                case 'vv':
case '❤️': {
    const ctxInfoVV = msg.message.extendedTextMessage?.contextInfo;
    const quotedRawVV = ctxInfoVV?.quotedMessage;
    if (!quotedRawVV) {
        await socket.sendMessage(sender, { text: '❌ View Once photo / video / audio ekakata *reply* karala .vv kiyala danna.' }, { quoted: msg });
        break;
    }

    try {
        await socket.sendMessage(sender, { react: { text: '🔄', key: msg.key } });

        const qVV = unwrap(quotedRawVV); // ephemeral / viewOnce v1, v2, v2Extension okkoma ain karanawa
        let mediaVV = null, typeVV = null;
        for (const [k, t] of [['imageMessage', 'image'], ['videoMessage', 'video'], ['audioMessage', 'audio']]) {
            if (qVV[k]) { mediaVV = qVV[k]; typeVV = t; break; }
        }

        if (mediaVV) {
            const streamVV = await downloadContentFromMessage(mediaVV, typeVV);
            const chunksVV = [];
            for await (const chunk of streamVV) chunksVV.push(chunk);
            const bufferVV = Buffer.concat(chunksVV);
            if (!bufferVV.length) throw new Error('Media eka download karanna baha (expire wela / already opened)');

            const capVV = `🔓 *View Once Restored*\n${mediaVV.caption ? `\n${mediaVV.caption}\n` : ''}\n> 🎭 *SHAGGY XMD* 🎭`;
            if (typeVV === 'image') {
                await socket.sendMessage(sender, { image: bufferVV, caption: capVV }, { quoted: msg });
            } else if (typeVV === 'video') {
                await socket.sendMessage(sender, { video: bufferVV, caption: capVV, mimetype: mediaVV.mimetype || 'video/mp4' }, { quoted: msg });
            } else {
                await socket.sendMessage(sender, { audio: bufferVV, mimetype: mediaVV.mimetype || 'audio/mp4', ptt: !!mediaVV.ptt }, { quoted: msg });
            }
        } else {
            // media nathnam - reply karapu kenage Profile Picture eka
            const targetJidVV = ctxInfoVV.participant || sender;
            let ppUrlVV;
            try {
                ppUrlVV = await socket.profilePictureUrl(targetJidVV, 'image');
            } catch {
                throw new Error('Meka View Once media ekak nemei, Profile Picture ekakuth ganna baha.');
            }
            await socket.sendMessage(sender, { image: { url: ppUrlVV }, caption: '👤 *Profile Picture*\n\n> 🎭 *SHAGGY XMD* 🎭' }, { quoted: msg });
        }
        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
    } catch (err) {
        console.error('[vv] error:', err.message);
        await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } }).catch(() => {});
        await socket.sendMessage(sender, { text: `❌ *VV Error:* ${err.message}` }, { quoted: msg });
    }
    break;
}
case 'cinbz':             
case 'cinetv': {
    const DEFAULT_FOOTER = `\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗠𝗢𝗩𝗜𝗘 𝗕𝗢𝗧 🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ ʟYɴᴋᴏ`;

    if (!args.length) {
        await socket.sendMessage(sender, {
            text: `*❪ ERROR ❫*\n\n⚠️ *Invalid Usage!*\n\n🎬 *Example:*
• .cinetv spider man
• .cinesubz game of thrones\n\n📝 _Please provide the Movie_ _or TV Series name!_${DEFAULT_FOOTER}`
        }, { quoted: msg });
        break;
    }

    const cinesubQuery = args.join(' ');
    await socket.sendMessage(sender, { 
        text: `*❪ SEARCHING ❫*\n\n🔍 *Searching Movies...*\n⚡ _Please wait a moment._`
    });

    const API_BASE = "https://api.chamindu.site";
    const API_KEY = "chama_api_11230a80e5eed3c1b80bfcc5d1773ec9"; // ඔබේ API Key එක දාන්න
    const DEFAULT_IMAGE = "https://api.chamindu.site/logo.png";

    try {
        const searchResponse = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/search?q=${encodeURIComponent(cinesubQuery)}&api_key=${API_KEY}`);
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                text: `*❪ NO RESULTS ❫*\n\n😞 *No Results Found!*\n\n🎬 *Query:* _${cinesubQuery}_\n💡 *Tip:* _Please check the spelling and try again!_${DEFAULT_FOOTER}`
            }, { quoted: msg });
            break;
        }

        const cinesubResults = searchData.data.slice(0, 25);
        let listText = `*❪ SEARCH RESULTS ❫*\n\n🎯 *Query:* _${cinesubQuery}_\n📊 *Results:* _${cinesubResults.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;

        cinesubResults.forEach((item, index) => {
            const typeIcon = item.type === 'tvshows' ? '📺' : '🎥';
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            listText += `*${num}* ➜ ${typeIcon} _${item.title.substring(0, 30)}_\n`;
        });

        listText += `${DEFAULT_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, { text: listText }, { quoted: msg });
        const messageID = sentMsg.key.id;

        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= cinesubResults.length) {
                    await socket.sendMessage(sender, {
                        text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 - ${cinesubResults.length}_\n📝 _Please reply with a valid number!_${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                    return;
                }

                const selectedItem = cinesubResults[choice];
                const isTvShow = selectedItem.type === 'tvshows';

                if (isTvShow) {
                    await socket.sendMessage(sender, { 
                        text: `*❪ FETCHING ❫*\n\n📺 *Fetching TV Series...*\n⚡ _Please wait..._`
                    }, { quoted: replyMek });

                    try {
                        const tvShowResponse = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/tv/info?q=${encodeURIComponent(selectedItem.link)}&api_key=${API_KEY}`);
                        const tvShowData = tvShowResponse.data;

                        if (!tvShowData.status || !tvShowData.data) {
                            throw new Error('Failed to fetch TV show details');
                        }

                        const tvInfo = tvShowData.data;

                        let tvDetailsText = `*❪ TV SERIES DETAILS ❫*\n\n📺 *${tvInfo.title}*\n⭐ 𝗜ᴍᴅʙ ➜ ★ ${tvInfo.rating || 'N/A'}\n📅 𝗬ᴇᴀʀ ➜ ${tvInfo.year || 'N/A'}\n⏳ 𝗥ᴜɴᴛɪᴍᴇ ➜ ${tvInfo.duration || 'N/A'}\n🌍 𝗖ᴏᴜɴ𝘁𝗿ʏ ➜ ${tvInfo.country || 'N/A'}\n🎭 𝗚𝗲𝗻 genres ➜ ${tvInfo.genres ? tvInfo.genres.join(', ') : 'N/A'}\n🎬 𝗗ɪʀᴇᴄᴛᴏʀ ➜ ${tvInfo.directors || 'N/A'}\n⭐ 𝗦ᴛᴀʀ𝘀: ${tvInfo.stars || 'N/A'}\n📝 𝗦𝘁𝗼𝗿𝘆 ➜ ${tvInfo.story ? (tvInfo.story.length > 250 ? tvInfo.story.substring(0, 250) + '...' : tvInfo.story) : 'N/A'}\n🗿 𝗪ᴇʙ ➜ cinesubz.com\n ${DEFAULT_FOOTER}`;

                        const posterUrl = tvInfo.image || selectedItem.image || DEFAULT_IMAGE;
                        await socket.sendMessage(sender, {
                            image: { url: posterUrl },
                            caption: tvDetailsText
                        }, { quoted: replyMek });

                        // AUTO DOWNLOAD ALL EPISODES
                        await socket.sendMessage(sender, { 
                            text: `*❪ DOWNLOAD EPISODES ❫*\n\n📺 *Series:* _${tvInfo.title}_\n🎬 *Episodes:* _${tvInfo.episodes.length}_\n⚡ _Starting download process..._${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });

                        let successCount = 0;
                        let failCount = 0;

                        for (let i = 0; i < tvInfo.episodes.length; i++) {
                            const episode = tvInfo.episodes[i];
                            try {
                                await socket.sendMessage(sender, { 
                                    text: `*❪ DOWNLOADING ❫*\n\n🎥 *Episode:* _${episode.episode_name}_\n📊 *Progress:* _${i + 1}/${tvInfo.episodes.length}_`
                                }, { quoted: replyMek });

                                const epDlRes = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/tv/dl?q=${encodeURIComponent(episode.episode_url)}&api_key=${API_KEY}`);
                                const epDlData = epDlRes.data;

                                if (epDlData.status && epDlData.data && epDlData.data.length > 0) {
                                    const nonTelegramLinks = epDlData.data.filter(link => 
                                        link.link && !link.link.includes('t.me') && !link.link.includes('telegram')
                                    );
                                    const finalLinkObj = nonTelegramLinks[0] || epDlData.data[0];

                                    await socket.sendMessage(sender, {
                                        document: { url: finalLinkObj.link },
                                        mimetype: 'video/mp4',
                                        fileName: `${tvInfo.title} - ${episode.episode_name}.mp4`,
                                        caption: `*📺 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗠𝗢𝗩𝗜𝗘 𝗕𝗢𝗧 📺*\n\n🎭 *Title:* ${tvInfo.title}\n📌 *Episode:* ${episode.episode_name}\n📊 *Quality:* Direct MP4\n\n${DEFAULT_FOOTER}`
                                    }, { quoted: replyMek });

                                    successCount++;
                                } else {
                                    failCount++;
                                }

                                await new Promise(resolve => setTimeout(resolve, 2500));

                            } catch (epError) {
                                console.error(`Error downloading episode:`, epError);
                                failCount++;
                            }
                        }

                        await socket.sendMessage(sender, { 
                            text: `*❪ SUMMARY ❫*\n\n🎉 *Download Complete!*\n\n🎬 *Series:* _${tvInfo.title}_\n✅ *Success:* _${successCount} Episodes_\n❌ *Failed:* _${failCount} Episodes_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });

                        socket.ev.off('messages.upsert', handleSelection);

                    } catch (tvShowError) {
                        console.error('TV Show error:', tvShowError);
                        await socket.sendMessage(sender, {
                            text: `*❪ ERROR ❫*\n\n❌ *TV Details Error!*\n🚫 _${tvShowError.message}_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }

                } else {
                    // MOVIE FLOW
                    await socket.sendMessage(sender, { 
                        text: `*❪ FETCHING ❫*\n\n🎬 *Fetching Movie...*\n⚡ _Please wait..._`
                    }, { quoted: replyMek });

                    try {
                        const detailsResponse = await axios.get(`${API_BASE}/api/v1/movie/cinesubz/infodl?q=${encodeURIComponent(selectedItem.link)}&api_key=${API_KEY}`);
                        const detailsData = detailsResponse.data;

                        if (!detailsData.status || !detailsData.data) {
                            throw new Error('Failed to fetch details');
                        }

                        const movieInfo = detailsData.data;
                        const validDownloads = movieInfo.downloads || [];

                        if (validDownloads.length === 0) {
                            await socket.sendMessage(sender, {
                                text: `*❪ NO DOWNLOADS ❫*\n\n⚠️ *No Downloads Found!*\n😞 _There are no downloads available for this movie!_${DEFAULT_FOOTER}`
                            }, { quoted: replyMek });
                            return;
                        }

                        const movieDetailsText = `*❪ MOVIE DETAILS ❫*\n\n🎬 *${movieInfo.title}*\n⭐ 𝗜𝗠𝗗𝗕 ➜ ★ ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n📅 𝗬𝗲𝗮𝗿 ➜ ${movieInfo.year || 'N/A'}\n⏳ 𝗗𝘂𝗿𝗮𝘁𝗶𝗼𝗻 ➜ ${movieInfo.duration || 'N/A'}\n🌍 𝗖ᴏᴜɴ𝘁𝗿ʏ ➜ ${movieInfo.country || 'N/A'}\n🎭 𝗚𝗲𝗻 genres ➜ ${movieInfo.genres ? movieInfo.genres.join(', ') : 'N/A'}\n🏷️  ➜ ${movieInfo.language || movieInfo.tag || 'N/A'}\n🎬  ➜ ${movieInfo.directors || movieInfo.director || 'N/A'}\n⭐  ➜ ${movieInfo.stars || 'N/A'}\n📝  ➜ ${movieInfo.story ? (movieInfo.story.length > 250 ? movieInfo.story.substring(0, 250) + '...' : movieInfo.story) : 'N/A'}\n🗿 𝗪ᴇʙ ➜ cinesubz.com\n ${DEFAULT_FOOTER}`;

                        const moviePosterUrl = movieInfo.image || selectedItem.image || DEFAULT_IMAGE;
                        await socket.sendMessage(sender, {
                            image: { url: moviePosterUrl },
                            caption: movieDetailsText
                        }, { quoted: replyMek });

                        const downloadOptionsText = `*❪ DOWNLOADS ❫*\n\n📥 *Select Quality:*\n\n${validDownloads.map((dl, i) => {
    const num = (i + 1) < 10 ? `0${i + 1}` : `${i + 1}`;
    const qualityIcon = (dl.quality || '').includes('1080') ? '🔥' : (dl.quality || '').includes('720') ? '💎' : '📱';
    return `*${num}* ➜ ${qualityIcon} _${dl.quality}_ 💾 _${dl.size || 'N/A'}_`;
}).join('\n')}\n\n*💬 REPLY TO DOWNLOAD 💬*\n📌 _Reply with the number_${DEFAULT_FOOTER}`;

                        const downloadOptionsMsg = await socket.sendMessage(sender, { text: downloadOptionsText }, { quoted: replyMek });
                        const optionsMsgID = downloadOptionsMsg.key.id;

                        const handleDownload = async ({ messages: downloadMessages }) => {
                            const downloadMek = downloadMessages[0];
                            if (!downloadMek?.message) return;

                            const downloadChoice = downloadMek.message.conversation || downloadMek.message.extendedTextMessage?.text;
                            const isReplyToOptionsMsg = downloadMek.message.extendedTextMessage?.contextInfo?.stanzaId === optionsMsgID;

                            if (isReplyToOptionsMsg && sender === downloadMek.key.remoteJid) {
                                const choiceNum = parseInt(downloadChoice) - 1;

                                if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= validDownloads.length) {
                                    await socket.sendMessage(sender, {
                                        text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 - ${validDownloads.length}_\n📝 _Please reply with a valid number!_${DEFAULT_FOOTER}`
                                    }, { quoted: downloadMek });
                                    return;
                                }

                                const selectedDownload = validDownloads[choiceNum];
                                await socket.sendMessage(sender, { react: { text: '📥', key: downloadMek.key } });

                                try {
                                    const finalDirectLink = selectedDownload.link;

                                    await socket.sendMessage(sender, {
                                        document: { url: finalDirectLink },
                                        mimetype: 'video/mp4',
                                        fileName: `${movieInfo.title} - ${selectedDownload.quality}.mp4`,
                                        caption: `*🎬 𝗦𝗛𝗔𝗚𝗚𝗬 𝗠𝗢𝗩𝗜𝗘 🎬*\n\n🎭 *Title:* ${movieInfo.title}\n🌟 *IMDB:* ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n📅 *Year:* ${movieInfo.year || 'N/A'}\n📊 *Quality:* ${selectedDownload.quality}\n💾 *Size:* ${selectedDownload.size || 'N/A'}\n\n${DEFAULT_FOOTER}`
                                    }, { quoted: downloadMek });

                                    await socket.sendMessage(sender, { react: { text: '✅', key: downloadMek.key } });

                                } catch (downloadError) {
                                    console.error('Download link error:', downloadError);
                                    await socket.sendMessage(sender, {
                                        text: `*❪ ERROR ❫*\n\n❌ *Download Failed!*\n🚫 _${downloadError.message}_${DEFAULT_FOOTER}`
                                    }, { quoted: downloadMek });
                                } finally {
                                    socket.ev.off('messages.upsert', handleDownload);
                                    socket.ev.off('messages.upsert', handleSelection);
                                }
                            }
                        };

                        socket.ev.on('messages.upsert', handleDownload);

                    } catch (detailsError) {
                        console.error('Details error:', detailsError);
                        await socket.sendMessage(sender, {
                            text: `*❪ ERROR ❫*\n\n❌ *Movie Details Error!*\n🚫 _${detailsError.message}_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }
                }
            }
        };

        socket.ev.on('messages.upsert', handleSelection);

    } catch (error) {
        console.error('Cinesubz command error:', error);
        await socket.sendMessage(sender, {
            text: `*❪ SYSTEM ERROR ❫*\n\n❌ *System Error!*\n🚫 _${error.message || 'Unknown error'}_\n\n🔄 _Please try again later..._${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }

    break;
}
 // ==========================================
// PUPILMOVIE - SHAGGY XMD Sinhala Dubbed Movies
// ==========================================
case 'pupilmovie':
case 'pupil': {
    const DEFAULT_FOOTER = `\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;
    const TEMP_DIR = './tmp_pupilmovie';

    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .pupilmovie spider*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const movieQueryF = args.join(' ').trim();
    const API_BASE = config.API_MAIN_URL || 'https://api-siteh-22e22e4cb068.herokuapp.com';
    const API_KEY = config.API_KEY || 'lakiya_2f3b6c382d1236ad7a08d56331fb679935d51dfc846df2c254093fd1fff9494e';

    let pupilSelectionListener = null;
    let pupilDownloadListener = null;
    let pupilMasterTimeout = null;

    const clearAllPupilListeners = () => {
        if (pupilSelectionListener) { socket.ev.off('messages.upsert', pupilSelectionListener); pupilSelectionListener = null; }
        if (pupilDownloadListener)  { socket.ev.off('messages.upsert', pupilDownloadListener);  pupilDownloadListener  = null; }
        if (pupilMasterTimeout)     { clearTimeout(pupilMasterTimeout); pupilMasterTimeout = null; }
    };

    const parseSizeMB = (s) => {
        if (!s) return 0;
        const m = s.toString().toUpperCase().replace(/\s/g, '').match(/([\d.]+)(GB|MB|KB)/);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2];
        if (u === 'GB') return v * 1024;
        if (u === 'MB') return v;
        return 0;
    };

    // ⭐ Server download
    const downloadToServer = async (url, dest) => {
        await fs.ensureDir(path.dirname(dest));
        const writer = fs.createWriteStream(dest);
        const res = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: 0,
            maxRedirects: 5,
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://pupilmovie.com/',
                'Accept': '*/*'
            }
        });
        res.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
            res.data.on('error', reject);
        });
    };

    try {
        await socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗦𝗘𝗔𝗥𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🔍 *Searching PupilVideo for:* _${movieQueryF}_\n⚡ _Please wait..._`
        }, { quoted: msg });

        // ═══ STEP 1 : SEARCH ═══
        const searchResponse = await axios.get(`${API_BASE}/pupilvideo/search?query=${encodeURIComponent(movieQueryF)}&api_key=${API_KEY}`, {
            timeout: 120000
        });
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.data?.results || searchData.data.results.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*කිසිදු චිත්‍රපටයක් හමු නොවීය!*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const movies = searchData.data.results.slice(0, 25);
        let listText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗣𝗨𝗣𝗜𝗟𝗩𝗜𝗗𝗘𝗢 ❫*\n\n🎯 *Query:* _${movieQueryF}_\n📊 *Results:* _${movies.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;

        movies.forEach((movie, index) => {
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            listText += `*${num}* ➜ 🎀 _${movie.title}*\n`;
        });

        listText += `\n📌 _Reply with number to download!_${DEFAULT_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const messageID = sentMsg.key.id;
        pupilMasterTimeout = setTimeout(clearAllPupilListeners, 180000);

        // ═══ STEP 2 : USER PICKS MOVIE ═══
        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= movies.length) {
                    return socket.sendMessage(sender, {
                        image: { url: pickBotImage(sessionConfig) },
                        caption: formatMessage(
                            '❌ INVALID SELECTION',
                            `*වැරදි අංකයක්! 1-${movies.length} අතර තෝරන්න!*`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                }

                if (pupilSelectionListener) { socket.ev.off('messages.upsert', pupilSelectionListener); pupilSelectionListener = null; }

                const selectedMovie = movies[choice];
                await socket.sendMessage(sender, { text: '📽️ *Fetching movie details...*' }, { quoted: replyMek });

                try {
                    // ═══ STEP 3 : INFO ═══
                    const infoResponse = await axios.get(`${API_BASE}/pupilvideo/movie?url=${encodeURIComponent(selectedMovie.url)}&api_key=${API_KEY}`, {
                        timeout: 120000
                    });
                    const infoData = infoResponse.data;

                    if (!infoData.status || !infoData.data) {
                        throw new Error('Failed to fetch movie details');
                    }

                    const movieInfo = infoData.data;
                    const allDownloadLinks = movieInfo.download_links || [];

                    if (allDownloadLinks.length === 0) {
                        return socket.sendMessage(sender, {
                            image: { url: pickBotImage(sessionConfig) },
                            caption: formatMessage(
                                '❌ NO DOWNLOADS',
                                '*බාගත කිරීම් හමු නොවීය!*',
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: replyMek });
                    }

                    const processedLinks = allDownloadLinks.map(link => {
                        const url = link.url || '';
                        if (url.includes('iws.sinhalachr.workers.dev') && !url.includes('download=true')) {
                            const separator = url.includes('?') ? '&' : '?';
                            return { ...link, url: url + separator + 'download=true' };
                        }
                        return link;
                    });

                    // Movie details
                    let detailsText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗣𝗨𝗣𝗜𝗟𝗩𝗜𝗗𝗘𝗢 ❫*\n\n`;
                    detailsText += `🎬 *${movieInfo.title || selectedMovie.title}*\n`;
                    if (movieInfo.metadata?.imdb_rating) detailsText += `⭐ *IMDb:* ${movieInfo.metadata.imdb_rating}\n`;
                    if (movieInfo.metadata?.year) detailsText += `📅 *Year:* ${movieInfo.metadata.year}\n`;
                    if (movieInfo.metadata?.runtime) detailsText += `⏳ *Runtime:* ${movieInfo.metadata.runtime}\n`;
                    if (movieInfo.categories?.length) detailsText += `🎭 *Genres:* ${movieInfo.categories.join(', ')}\n`;
                    if (movieInfo.author) detailsText += `👨‍💻 *Author:* ${movieInfo.author}\n`;
                    detailsText += `\n`;
                    if (movieInfo.description) detailsText += `📖 *Story:*\n_${movieInfo.description.substring(0, 200)}..._\n`;
                    detailsText += DEFAULT_FOOTER;

                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: movieInfo.poster || pickBotImage(sessionConfig) },
                        caption: detailsText
                    }, { quoted: replyMek });

                    // ═══ STEP 4 : DOWNLOAD OPTIONS ═══
                    let dlText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗦 ❫*\n\n📥 *Select Quality:*\n\n`;
                    processedLinks.slice(0, 20).forEach((d, i) => {
                        const num = (i + 1) < 10 ? `0${i + 1}` : `${i + 1}`;
                        let platformEmoji = '📥';
                        if (d.url.includes('t.me/')) platformEmoji = '📱';
                        if (d.url.includes('cloud.sinhalachr.workers.dev')) platformEmoji = '☁️';
                        if (d.url.includes('iws.sinhalachr.workers.dev')) platformEmoji = '🌐';
                        dlText += `*${num}* ➜ ${platformEmoji} _${d.quality || 'Unknown'}_ (${d.file_size || 'N/A'})\n`;
                    });
                    dlText += `\n📌 _Reply with number to send file._${DEFAULT_FOOTER}`;

                    const downloadMsg = await socket.sendMessage(sender, { text: dlText }, { quoted: infoMsg });
                    const infoMsgID = downloadMsg.key.id;

                    // ═══ STEP 5 : USER PICKS DOWNLOAD ═══
                    const handleDownload = async ({ messages: downloadMessages }) => {
                        const downloadMek = downloadMessages[0];
                        if (!downloadMek?.message) return;

                        const downloadChoice = downloadMek.message.conversation || downloadMek.message.extendedTextMessage?.text;
                        const isReplyToInfoMsg = downloadMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;

                        if (isReplyToInfoMsg && sender === downloadMek.key.remoteJid) {
                            const choiceNum = parseInt(downloadChoice) - 1;

                            if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= processedLinks.length) {
                                return socket.sendMessage(sender, {
                                    image: { url: pickBotImage(sessionConfig) },
                                    caption: formatMessage(
                                        '❌ INVALID SELECTION',
                                        `*වැරදි අංකයක්! 1-${processedLinks.length} අතර තෝරන්න!*`,
                                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                    )
                                }, { quoted: downloadMek });
                            }

                            clearAllPupilListeners();
                            const selectedDownload = processedLinks[choiceNum];
                            const downloadUrl = selectedDownload.url;
                            const sizeMB = parseSizeMB(selectedDownload.file_size);

                            await socket.sendMessage(sender, { react: { text: '📥', key: downloadMek.key } });

                            // 🔗 Telegram → link only
                            if (downloadUrl.includes('t.me/')) {
                                return socket.sendMessage(sender, {
                                    text: `📱 *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗧𝗘𝗟𝗘𝗚𝗥𝗔𝗠*\n\n🎬 *${movieInfo.title}*\n📌 *${selectedDownload.quality || 'HD'}*\n\n🔗 *Telegram Link:*\n${downloadUrl}\n\n_Telegram bot එකෙන් download කරන්න._${DEFAULT_FOOTER}`
                                }, { quoted: downloadMek });
                            }

                            // ⚠️ 2GB limit
                            if (sizeMB > 2000) {
                                return socket.sendMessage(sender, {
                                    text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${movieInfo.title}*\n📌 *${selectedDownload.quality}*\n📦 *${selectedDownload.file_size}*\n\n🔗 *Direct Link:*\n${downloadUrl}\n\n_IDM එකෙන් download කරන්න._${DEFAULT_FOOTER}`
                                }, { quoted: downloadMek });
                            }

                            await socket.sendMessage(sender, {
                                text: `⏳ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚*\n\n📌 *${selectedDownload.quality || 'HD'}*\n📦 *Size:* ${selectedDownload.file_size || 'N/A'}\n\n_කරුණාකර රැඳී සිටින්න..._`
                            }, { quoted: downloadMek });

                            // ⭐ Server download
                            await fs.ensureDir(TEMP_DIR);
                            const safeName = (movieInfo.title || selectedMovie.title).replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 50);
                            const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.mp4`);

                            try {
                                await downloadToServer(downloadUrl, localFile);

                                const stats = await fs.stat(localFile);
                                const realSizeMB = stats.size / 1024 / 1024;

                                // ⚠️ Error page check
                                if (realSizeMB < 1) {
                                    await fs.remove(localFile).catch(() => {});
                                    throw new Error('Download failed — file too small (error page detected)');
                                }

                                await socket.sendMessage(sender, {
                                    text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                                }, { quoted: downloadMek });

                                // ⭐ Send as document
                                const fileName = `${safeName} - ${selectedDownload.quality || 'HD'}.mp4`;

                                try {
                                    await socket.sendMessage(sender, {
                                        document: { url: localFile },
                                        mimetype: 'video/mp4',
                                        fileName: fileName,
                                        caption: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗣𝗨𝗣𝗜𝗟𝗩𝗜𝗗𝗘𝗢*\n\n🎬 *Title:* ${movieInfo.title || selectedMovie.title}\n📅 *Year:* ${movieInfo.metadata?.year || 'N/A'}\n📌 *Quality:* ${selectedDownload.quality || 'HD'}\n📦 *Size:* ${selectedDownload.file_size || 'N/A'}\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
                                    }, { quoted: downloadMek });

                                    await socket.sendMessage(sender, { react: { text: '✅', key: downloadMek.key } });

                                } catch (sendErr) {
                                    await socket.sendMessage(sender, {
                                        text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${downloadUrl}\n\n_IDM එකෙන් download කරන්න._`
                                    }, { quoted: downloadMek });
                                }

                                // Cleanup
                                await fs.remove(localFile).catch(() => {});

                            } catch (downloadErr) {
                                console.error('[PupilMovie] download error:', downloadErr.message);
                                await socket.sendMessage(sender, {
                                    text: `❌ *Download Error:* _${downloadErr.message}_\n\n🔗 *Direct Link:*\n${downloadUrl}\n\n💡 _IDM එකෙන් download කරන්න._`
                                }, { quoted: downloadMek });

                                try { await fs.remove(localFile); } catch {}
                            }
                        }
                    };

                    pupilDownloadListener = handleDownload;
                    socket.ev.on('messages.upsert', pupilDownloadListener);

                } catch (infoError) {
                    clearAllPupilListeners();
                    console.error('Movie info error:', infoError);
                    await socket.sendMessage(sender, {
                        image: { url: pickBotImage(sessionConfig) },
                        caption: formatMessage(
                            '❌ ERROR',
                            `*Movie details ලබාගැනීමේ දෝෂයක්:* ${infoError.message}`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                }
            }
        };

        pupilSelectionListener = handleSelection;
        socket.ev.on('messages.upsert', pupilSelectionListener);

    } catch (error) {
        clearAllPupilListeners();
        console.error('PupilMovie command error:', error);
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                `*දෝෂයක් ඇතිවුණා:* ${error.message || 'Unknown error'}`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }
    break;
} 
// ==========================================
// MOVIESUBLK.COM - SHAGGY XMD (GDrive + Direct)
// ==========================================
case 'moviesublk':
case 'msubz':
case 'mslk': {
    const DEFAULT_FOOTER = `\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;
    const TEMP_DIR = './tmp_moviesublk';

    if (!args.length) {
        return socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .moviesublk Avatar*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }

    const movieQuery = args.join(' ').trim();
    const API_BASE = 'https://api.chamindu.site/api/v1/movies/moviesublkcom';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    const TIMEOUT_API = 120000;
    const TIMEOUT_INFO = 120000;

    let msubzSelectionListener = null;
    let msubzDownloadListener = null;
    let msubzMasterTimeout = null;

    const clearAllMsubzListeners = () => {
        if (msubzSelectionListener) { socket.ev.off('messages.upsert', msubzSelectionListener); msubzSelectionListener = null; }
        if (msubzDownloadListener)  { socket.ev.off('messages.upsert', msubzDownloadListener);  msubzDownloadListener  = null; }
        if (msubzMasterTimeout)     { clearTimeout(msubzMasterTimeout); msubzMasterTimeout = null; }
    };

    const msubzErr = (e) => {
        if (e.response) {
            const d = e.response.data;
            const apiMsg = (d && (d.message || d.error)) || '';
            return `API error (HTTP ${e.response.status}) ${typeof apiMsg === 'string' ? apiMsg : ''}`.trim();
        }
        if (/timeout/i.test(e.message)) return 'API එක slow නිසා timeout වුනා (2 min). නැවත try කරන්න.';
        if (e.code === 'ENOTFOUND' || e.code === 'ECONNREFUSED') return `API server එකට connect වෙන්න බෑ (${e.code}).`;
        return e.message;
    };

    const cleanMsubzTitle = (t = '') =>
        t.replace(/\s*\|\s*සිංහල උපසිරැසි.*$/i, '')
         .replace(/\s*Sinhala Subtitles.*$/i, '')
         .replace(/\s*Sinhala Dubbed.*$/i, '')
         .replace(/\s*\|.*$/i, '')
         .trim();

    try {
        await socket.sendMessage(sender, {
            text: `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 𝗦𝗘𝗔𝗥𝗖𝗛𝗜𝗡𝗚 ❫*\n\n🔍 *Searching MovieSubLK for:* _${movieQuery}_\n⚡ _Please wait..._`
        }, { quoted: msg });

        // ═══ STEP 1 : SEARCH ═══
        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: movieQuery, api_key: API_KEY },
            timeout: TIMEOUT_API
        });

        const searchData = searchRes.data;
        const results = searchData.data || [];

        if (!searchData.status || results.length === 0) {
            return socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage('❌ NO RESULTS', '*කිසිදු චිත්‍රපටයක් හමු නොවීය!*', `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
            }, { quoted: msg });
        }

        const list = results.slice(0, 20);
        let listText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗠𝗢𝗩𝗜𝗘𝗦𝗨𝗕𝗟𝗞 ❫*\n\n🎯 *Query:* _${movieQuery}_\n📊 *Results:* _${list.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;

        list.forEach((item, index) => {
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            const typeIcon = item.type === 'tvshows' ? '📺' : '🎥';
            listText += `*${num}* ➜ ${typeIcon} _${cleanMsubzTitle(item.clean_title || item.title)}_\n`;
        });
        listText += `\n📌 _Reply with number to download!_${DEFAULT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: list[0].image || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;
        msubzMasterTimeout = setTimeout(clearAllMsubzListeners, 180000);

        // ═══ STEP 2 : USER PICKS ═══
        const handleMovieSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || !sameChat(replyMek.key, sender)) return;
            const { text } = extractIncoming(replyMek);
            if (!isReplyTo(replyMek, searchMsgID)) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= list.length) {
                return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${list.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: replyMek });
            }

            if (msubzSelectionListener) { socket.ev.off('messages.upsert', msubzSelectionListener); msubzSelectionListener = null; }

            const chosen = list[choice];
            await socket.sendMessage(sender, { text: '⏳ Fetching movie details & download links...' }, { quoted: replyMek });

            try {
                // ═══ STEP 3 : INFO ═══
                const infoRes = await axios.get(`${API_BASE}/infodl`, {
                    params: { q: chosen.link, api_key: API_KEY },
                    timeout: TIMEOUT_INFO
                });

                const infoData = infoRes.data?.data;
                const downloads = infoData?.downloads || [];
                const episodes = infoData?.episodes || [];
                const isTv = infoData?.is_tv || episodes.length > 0;

                if (!infoData) throw new Error('Movie details හමු නොවීය.');
                if (downloads.length === 0 && episodes.length === 0) throw new Error('Download links හමු නොවීය.');

                let infoText = `*❪ 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗘𝗧𝗔𝗜𝗟𝗦 ❫*\n\n`;
                infoText += `🎬 *${cleanMsubzTitle(infoData.title || chosen.clean_title || chosen.title)}*\n`;
                if (infoData.year) infoText += `📅 *Year:* ${infoData.year}\n`;
                if (infoData.imdb_rating) infoText += `⭐ *IMDb:* ${infoData.imdb_rating}\n`;
                if (infoData.director) infoText += `🎬 *Director:* ${infoData.director}\n`;
                if (infoData.genre) infoText += `🎭 *Genres:* ${infoData.genre}\n`;
                infoText += `\n`;

                if (infoData.storyline) {
                    infoText += `📖 *Story:*\n_${infoData.storyline.substring(0, 200)}..._\n\n`;
                }

                if (isTv && episodes.length > 0) {
                    infoText += `📺 *Type:* TV Series\n`;
                    infoText += `🔢 *Total Episodes:* ${episodes.length}\n\n`;
                    infoText += `*Available Episodes:*\n`;
                    episodes.forEach((ep, i) => {
                        infoText += `*${i + 1}.* E${ep.episode_number} - ${ep.title || 'Episode'}\n`;
                    });
                    infoText += `\n👉 *බාගත කිරීමට Episode අංකය Reply කරන්න.*`;
                } else {
                    infoText += `🎥 *Type:* Movie\n\n`;
                    infoText += `*Available Downloads:*\n`;
                    downloads.forEach((dl, i) => {
                        const isTg = dl.link?.includes('t.me/');
                        const sizeMB = parseSizeMB(dl.size);
                        let note = '';
                        if (isTg) note = ' 🔗';
                        else if (sizeMB > 2000) note = ' ⚠️';
                        else note = ' ✓';
                        infoText += `*${i + 1}.* ${dl.title || dl.quality}${note}\n`;
                    });
                    infoText += `\n👉 *බාගත කිරීමට අදාළ අංකය Reply කරන්න.*\n_✓ = Document • 🔗 = Telegram • ⚠️ = 2GB+_`;
                }

                const infoMsg = await socket.sendMessage(sender, {
                    image: { url: infoData.image || chosen.image || pickBotImage(sessionConfig) },
                    caption: infoText
                }, { quoted: replyMek });

                const infoMsgID = infoMsg.key.id;

                // ═══ STEP 4 : USER PICKS DOWNLOAD ═══
                const handleDownloadSelection = async ({ messages: dlMessages }) => {
                    const dlMek = dlMessages?.[0];
                    if (!dlMek?.message || !sameChat(dlMek.key, sender)) return;
                    const { text: dlChoiceText } = extractIncoming(dlMek);
                    if (!isReplyTo(dlMek, infoMsgID)) return;

                    const dlIdx = parseInt(dlChoiceText) - 1;

                    let selectedDl = null;
                    if (isTv && episodes.length > 0) {
                        if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= episodes.length) {
                            return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${episodes.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: dlMek });
                        }
                        const selectedEp = episodes[dlIdx];
                        const epDownloads = selectedEp.downloads || [];
                        if (epDownloads.length === 0) {
                            return socket.sendMessage(sender, { text: `❌ Episode ${selectedEp.episode_number} සඳහා download links නෑ.` }, { quoted: dlMek });
                        }
                        selectedDl = epDownloads[0];
                    } else {
                        if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= downloads.length) {
                            return socket.sendMessage(sender, { text: `❌ කරුණාකර 1 - ${downloads.length} අතර අංකයක් ලබාදෙන්න!` }, { quoted: dlMek });
                        }
                        selectedDl = downloads[dlIdx];
                    }

                    clearAllMsubzListeners();

                    const dlUrl = selectedDl.link;
                    const isTelegram = dlUrl.includes('t.me/');
                    const sizeMB = parseSizeMB(selectedDl.size);

                    await socket.sendMessage(sender, { react: { text: '📥', key: dlMek.key } });

                    // 🔗 Telegram
                    if (isTelegram) {
                        return socket.sendMessage(sender, {
                            text: `📱 *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗧𝗘𝗟𝗘𝗚𝗥𝗔𝗠*\n\n🎬 *${cleanMsubzTitle(infoData.title || chosen.clean_title)}*\n📌 *${selectedDl.title || selectedDl.quality}*\n\n🔗 *Telegram Link:*\n${dlUrl}\n\n_Telegram bot එකෙන් download කරන්න._${DEFAULT_FOOTER}`
                        }, { quoted: dlMek });
                    }

                    // ⚠️ 2GB limit
                    if (sizeMB > 2000) {
                        return socket.sendMessage(sender, {
                            text: `⚠️ *File එක 2GB ඉක්මවයි!*\n\n🎬 *${cleanMsubzTitle(infoData.title || chosen.clean_title)}*\n📌 *${selectedDl.quality}*\n📦 *${selectedDl.size}*\n\n🔗 *Direct Link:*\n${dlUrl}\n\n_IDM එකෙන් download කරන්න._${DEFAULT_FOOTER}`
                        }, { quoted: dlMek });
                    }

                    await socket.sendMessage(sender, {
                        text: `⏳ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚*\n\n📌 *${selectedDl.title || selectedDl.quality}*\n📦 *Size:* ${selectedDl.size || 'N/A'}\n\n_කරුණාකර රැඳී සිටින්න..._`
                    }, { quoted: dlMek });

                    await fs.ensureDir(TEMP_DIR);
                    const safeName = cleanMsubzTitle(infoData.title || chosen.clean_title || chosen.title).replace(/[^a-zA-Z0-9 ]/g, '_').substring(0, 50);
                    const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.mp4`);

                    try {
                        await downloadSmart(dlUrl, localFile);

                        const stats = await fs.stat(localFile);
                        const realSizeMB = stats.size / 1024 / 1024;

                        if (realSizeMB < 1) {
                            await fs.remove(localFile).catch(() => {});
                            throw new Error('File too small (error page)');
                        }

                        await socket.sendMessage(sender, {
                            text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                        }, { quoted: dlMek });

                        const fileName = `${safeName} - ${selectedDl.quality || 'HD'}.mp4`;

                        try {
                            await socket.sendMessage(sender, {
                                document: { url: localFile },
                                mimetype: 'video/mp4',
                                fileName: fileName,
                                caption: `✅ *𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 • 𝗠𝗢𝗩𝗜𝗘𝗦𝗨𝗕𝗟𝗞*\n\n🎬 *Title:* ${cleanMsubzTitle(infoData.title || chosen.clean_title)}\n📅 *Year:* ${infoData.year || 'N/A'}\n📌 *Quality:* ${selectedDl.quality || 'HD'}\n📦 *Size:* ${realSizeMB.toFixed(1)} MB\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
                            }, { quoted: dlMek });

                            await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });

                        } catch (sendErr) {
                            await socket.sendMessage(sender, {
                                text: `❌ *Send fail:* ${sendErr.message}\n\n🔗 *Direct Link:*\n${dlUrl}\n\n_IDM එකෙන් download කරන්න._`
                            }, { quoted: dlMek });
                        }

                        await fs.remove(localFile).catch(() => {});

                    } catch (downloadErr) {
                        console.error('[MovieSubLK] download error:', downloadErr.message);

                        await socket.sendMessage(sender, {
                            text: `⚠️ *Direct Download*\n\n🎬 *${cleanMsubzTitle(infoData.title || chosen.clean_title)}*\n📌 *${selectedDl.quality}*\n📦 *${selectedDl.size || 'N/A'}*\n\n🔗 *Download Link:*\n${dlUrl}\n\n💡 _IDM එකෙන් download කරන්න._\n\n> 🎭 𝗦𝗛𝗔𝗚𝗚𝗬 𝗫𝗠𝗗 🎭`
                        }, { quoted: dlMek });

                        try { await fs.remove(localFile); } catch {}
                    }
                };

                msubzDownloadListener = handleDownloadSelection;
                socket.ev.on('messages.upsert', msubzDownloadListener);
                if (msubzMasterTimeout) clearTimeout(msubzMasterTimeout);
                msubzMasterTimeout = setTimeout(clearAllMsubzListeners, 600000); // 10 min

            } catch (infoErr) {
                clearAllMsubzListeners();
                let errMsg = msubzErr(infoErr);
                await socket.sendMessage(sender, { text: `❌ MovieSubLK Info Error: ${errMsg}` }, { quoted: replyMek });
            }
        };

        msubzSelectionListener = handleMovieSelection;
        socket.ev.on('messages.upsert', msubzSelectionListener);

    } catch (err) {
        clearAllMsubzListeners();
        let errMsg = msubzErr(err);
        await socket.sendMessage(sender, { text: `❌ Error: ${errMsg}` }, { quoted: msg });
    }
    break;
}
  case 'rexporn':
case 'rxporn':
case 'rp': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '🔞 REXPORN SEARCH',
                '*කරුණාකර search කිරීමට keyword එකක් දෙන්න!*\n\n*📌 Usage:* `.rexporn stepmom`\n*📌 Usage:* `.rexporn milf 18+`',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const rpQuery = args.join(' ');
    const RP_API_BASE = 'https://api.chamindu.site/api/adult/rexporn';
    const RP_API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    let rpSelectionListener = null;
    let rpQualityListener = null;
    let rpMasterTimeout = null;

    const clearAllRPListeners = () => {
        if (rpSelectionListener) {
            socket.ev.off('messages.upsert', rpSelectionListener);
            rpSelectionListener = null;
        }
        if (rpQualityListener) {
            socket.ev.off('messages.upsert', rpQualityListener);
            rpQualityListener = null;
        }
        if (rpMasterTimeout) {
            clearTimeout(rpMasterTimeout);
            rpMasterTimeout = null;
        }
    };

    try {
        await socket.sendMessage(sender, {
            text: '🔍 *RexPorn* හි සොයමින්...'
        }, { quoted: msg });

        const searchRes = await axios.get(`${RP_API_BASE}/search`, {
            params: { q: rpQuery, page: 1, api_key: RP_API_KEY },
            timeout: 120000
        });

        const searchData = searchRes.data;
        if (!searchData.success || !searchData.results || searchData.results.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    `*"${rpQuery}"* සඳහා ප්‍රතිඵල හමු නොවීය!`,
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const rpList = searchData.results.slice(0, 20);

        let listText = `🔞 *𝗥𝗘𝗫𝗣𝗢𝗥𝗡 𝗦𝗘𝗔𝗥𝗖𝗛 : _${rpQuery}_*\n╭──────●➤\n*🔢 ʀᴇ𝗽𝗹ʏ ʙᴇʟ𝗼ᴡ ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        rpList.forEach((item, index) => {
            listText += `*🎬 ${index + 1} ┃❭❭ ${item.title}*\n    ↳ (⏱ ${item.duration || 'N/A'} | 🎞 ${item.quality || 'HD'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: rpList[0].thumbnail || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        rpMasterTimeout = setTimeout(() => {
            clearAllRPListeners();
        }, 120000);

        const handleRPSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;

            if (!isReply) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= rpList.length) {
                await socket.sendMessage(sender, {
                    text: `❌ කරුණාකර 1 - ${rpList.length} අතර අංකයක් ලබාදෙන්න!`
                }, { quoted: replyMek });
                return;
            }

            if (rpSelectionListener) {
                socket.ev.off('messages.upsert', rpSelectionListener);
                rpSelectionListener = null;
            }

            const chosenVideo = rpList[choice];
            await socket.sendMessage(sender, {
                text: `⏳ *"${chosenVideo.title}"* ගේ stream links ලබා ගනිමින්...`
            }, { quoted: replyMek });

            try {
                const dlRes = await axios.get(`${RP_API_BASE}/dl`, {
                    params: { url: chosenVideo.url, api_key: RP_API_KEY },
                    timeout: 120000
                });

                const dlData = dlRes.data;
                if (!dlData.success || !dlData.streams || dlData.streams.length === 0) {
                    throw new Error('Stream links හමු නොවීය. ☹️');
                }

                const streams = dlData.streams;

                let qualityText = `🔞 *${dlData.title}*\n\n`;
                qualityText += `⏱ *Duration:* ${dlData.duration || 'N/A'}\n\n`;
                qualityText += `*🎞 Available Qualities:*\n╭──────●➤\n`;

                streams.forEach((s, i) => {
                    const sizeMB = s.size_bytes ? (parseInt(s.size_bytes) / (1024 * 1024)).toFixed(0) + ' MB' : 'N/A';
                    qualityText += `*${i + 1}.* ${s.label}  ┃  📦 ~${sizeMB}\n`;
                });

                qualityText += `╰──────────●➤\n\n👉 *Quality reply කරන්න (1 = Best)*\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

                const qualityMsg = await socket.sendMessage(sender, {
                    image: { url: dlData.thumbnail || chosenVideo.thumbnail || pickBotImage(sessionConfig) },
                    caption: qualityText
                }, { quoted: replyMek });

                const qualityMsgID = qualityMsg.key.id;

                const handleQualitySelection = async ({ messages: qMsgs }) => {
                    const qMek = qMsgs?.[0];
                    if (!qMek?.message || qMek.key.remoteJid !== sender) return;

                    const qText = (qMek.message.conversation || qMek.message.extendedTextMessage?.text || '').trim();
                    const isQReply = qMek.message.extendedTextMessage?.contextInfo?.stanzaId === qualityMsgID;

                    if (!isQReply) return;

                    const qIdx = parseInt(qText) - 1;
                    if (isNaN(qIdx) || qIdx < 0 || qIdx >= streams.length) {
                        await socket.sendMessage(sender, {
                            text: `❌ කරුණාකර 1 - ${streams.length} අතර Quality අංකයක් ලබාදෙන්න!`
                        }, { quoted: qMek });
                        return;
                    }

                    clearAllRPListeners();

                    const selectedStream = streams[qIdx];
                    const downloadLink = selectedStream.download_url || selectedStream.stream_url;
                    const sizeMB = selectedStream.size_bytes
                        ? (parseInt(selectedStream.size_bytes) / (1024 * 1024)).toFixed(0) + ' MB'
                        : 'N/A';

                    await socket.sendMessage(sender, { react: { text: '📥', key: qMek.key } });

                    await socket.sendMessage(sender, {
                        text: `⏳ *Downloading:* ${dlData.title}\n📦 *Size:* ~${sizeMB} | 🎞 *Quality:* ${selectedStream.label}\n\n_කරුණාකර ටික වේලාවක් රැඳී සිටින්න..._`
                    }, { quoted: qMek });

                    const cleanTitle = dlData.title.replace(/[^a-zA-Z0-9 ]/g, '').trim().substring(0, 60);
                    const fileName = `${cleanTitle}_${selectedStream.quality || 'HD'}.mp4`;

                    try {
                        await socket.sendMessage(sender, {
                            document: { url: downloadLink },
                            mimetype: 'video/mp4',
                            fileName: fileName,
                            caption: `✅ *🔞 REXPORN DOWNLOAD*\n\n🎬 *Title:* ${dlData.title}\n🎞 *Quality:* ${selectedStream.label}\n📦 *Size:* ~${sizeMB}\n⏱ *Duration:* ${dlData.duration || 'N/A'}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        }, { quoted: qMek });

                        await socket.sendMessage(sender, { react: { text: '✅', key: qMek.key } });

                    } catch (uploadErr) {
                        await socket.sendMessage(sender, {
                            text: `❌ Upload Error: ${uploadErr.message}\n\n🔗 *Direct Link:*\n${downloadLink}`
                        }, { quoted: qMek });
                        await socket.sendMessage(sender, { react: { text: '❌', key: qMek.key } });
                    }
                };

                rpQualityListener = handleQualitySelection;
                socket.ev.on('messages.upsert', handleQualitySelection);

            } catch (dlErr) {
                clearAllRPListeners();
                await socket.sendMessage(sender, {
                    text: `❌ Download Info Error: ${dlErr.message}`
                }, { quoted: replyMek });
            }
        };

        rpSelectionListener = handleRPSelection;
        socket.ev.on('messages.upsert', handleRPSelection);

    } catch (err) {
        clearAllRPListeners();
        await socket.sendMessage(sender, {
            text: `❌ RexPorn Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
}                                                      

 case 'movieall':
case 'm': {
    const DEFAULT_FOOTER = `\n\n> 🎭 🅢🅗🅐🅖🅖🅨 🅧🅜🅓 🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ ʟYɴᴋᴏ`;

    if (!args.length) {
        await socket.sendMessage(sender, {
            text: `*❪ ERROR ❫*\n\n⚠️ *Invalid Usage!*\n\n🎬 *Example:*\n• .movie avatar\n• .m game of thrones\n\n📝 _Please provide the Movie_ _or TV Series name!_${DEFAULT_FOOTER}`
        }, { quoted: msg });
        break;
    }

    const query = args.join(' ');
    await socket.sendMessage(sender, { 
        text: `*❪ SEARCHING ❫*\n\n🔍 *Searching across all sources...*\n⚡ _Please wait a moment._`
    });

    const API_BASE = "https://api.chamindu.site";
    const API_KEY = "chama_api_11230a80e5eed3c1b80bfcc5d1773ec9"; // ඔබේ API Key එක දාන්න
    const DEFAULT_IMAGE = "https://api.chamindu.site/logo.png";

    try {
        const sites = ["cinesubz", "sinhalasub", "thenkiri", "moviesublk", "baiscope", "cineru"];
        const promises = sites.map(site => 
            axios.get(`${API_BASE}/api/v1/movie/${site}/search?q=${encodeURIComponent(query)}&api_key=${API_KEY}`)
                .then(res => res.data.status && res.data.data ? res.data.data.map(item => ({ ...item, site })) : [])
                .catch(() => [])
        );

        const resultsArrays = await Promise.all(promises);
        const results = resultsArrays.flat().slice(0, 25);

        if (results.length === 0) {
            await socket.sendMessage(sender, {
                text: `*❪ NO RESULTS ❫*\n\n😞 *No Results Found!*\n\n🎬 *Query:* _${query}_\n💡 *Tip:* _Please check the spelling and try again!_${DEFAULT_FOOTER}`
            }, { quoted: msg });
            break;
        }

        let listText = `*❪ MULTI-SOURCE SEARCH RESULTS ❫*\n\n🎯 *Query:* _${query}_\n📊 *Results:* _        ext ${results.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;

        results.forEach((item, index) => {
            const siteTag = item.site.toUpperCase();
            const typeIcon = item.type === 'tvshows' ? '📺' : '🎥';
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            listText += `*${num}* ➜ ${typeIcon} [_${siteTag}_] _${item.title.substring(0, 25)}_\n`;
        });

        listText += `${DEFAULT_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, { text: listText }, { quoted: msg });
        const messageID = sentMsg.key.id;

        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= results.length) {
                    await socket.sendMessage(sender, {
                        text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 -         ext ${results.length}_\n📝 _Please reply with a valid number!_${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                    return;
                }

                const selectedItem = results[choice];
                const site = selectedItem.site;
                const isTvShow = selectedItem.type === 'tvshows';

                if (isTvShow) {
                    await socket.sendMessage(sender, { 
                        text: `*❪ FETCHING ❫*\n\n📺 *Fetching TV Series details from ${site.toUpperCase()}...*\n⚡ _Please wait..._`
                    }, { quoted: replyMek });

                    try {
                        const tvShowResponse = await axios.get(`${API_BASE}/api/v1/movie/${site}/tv/info?q=${encodeURIComponent(selectedItem.link)}&api_key=${API_KEY}`);
                        const tvShowData = tvShowResponse.data;

                        if (!tvShowData.status || !tvShowData.data) {
                            throw new Error('Failed to fetch TV show details');
                        }

                        const tvInfo = tvShowData.data;

                        let tvDetailsText = `*❪ TV SERIES DETAILS ❫*\n\n📺 *${tvInfo.title}*\n⭐ 𝗜ᴍᴅ𝗯 ➜ ★ ${tvInfo.rating || 'N/A'}\n📅 𝗬ᴇᴀʀ ➜ ${tvInfo.year || 'N/A'}\n⏳ 𝗥ᴜɴᴛɪᴍᴇ ➜ ${tvInfo.duration || 'N/A'}\n🌍 🇨🇴🇺🇳🇹🇷🇾 ➜ ${tvInfo.country || 'N/A'}\n🎭 𝗚𝗲𝗻𝗴𝗿𝗲𝘀 ➜ ${tvInfo.genres ? tvInfo.genres.join(', ') : 'N/A'}\n?? 𝗦𝘁𝗼𝗿𝘆 ➜ ${tvInfo.story ? (tvInfo.story.length > 250 ? tvInfo.story.substring(0, 250) + '...' : tvInfo.story) : 'N/A'}\n🗿 𝗦𝗼𝘂𝗿𝗰𝗲 ➜ ${site.toUpperCase()}\n ${DEFAULT_FOOTER}`;

                        const posterUrl = tvInfo.image || selectedItem.image || DEFAULT_IMAGE;
                        await socket.sendMessage(sender, {
                            image: { url: posterUrl },
                            caption: tvDetailsText
                        }, { quoted: replyMek });

                        // AUTO DOWNLOAD ALL EPISODES
                        await socket.sendMessage(sender, { 
                            text: `*❪ DOWNLOAD EPISODES ❫*\n\n📺 *Series:* _${tvInfo.title}_
🎬 *Episodes:* _${tvInfo.episodes.length}_
⚡ _Starting download process..._${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });

                        let successCount = 0;
                        let failCount = 0;

                        for (let i = 0; i < tvInfo.episodes.length; i++) {
                            const episode = tvInfo.episodes[i];
                            try {
                                await socket.sendMessage(sender, { 
                                    text: `*❪ DOWNLOADING ❫*\n\n🎥 *Episode:* _${episode.episode_name || episode.name || 'Episode ' + (i + 1)}_
📊 *Progress:* _${i + 1}/${tvInfo.episodes.length}_`
                                }, { quoted: replyMek });

                                const epUrl = episode.episode_url || episode.url || episode.link;
                                const epDlRes = await axios.get(`${API_BASE}/api/v1/movie/${site}/tv/dl?q=${encodeURIComponent(epUrl)}&api_key=${API_KEY}`);
                                const epDlData = epDlRes.data;

                                if (epDlData.status && epDlData.data && epDlData.data.length > 0) {
                                    const nonTelegramLinks = epDlData.data.filter(link => 
                                        link.link && !link.link.includes('t.me') && !link.link.includes('telegram')
                                    );
                                    const finalLinkObj = nonTelegramLinks[0] || epDlData.data[0];

                                    let jpegThumbnail = undefined;
                                    try {
                                        const thumbRes = await axios.get(posterUrl, { responseType: 'arraybuffer' });
                                        jpegThumbnail = Buffer.from(thumbRes.data).toString('base64');
                                    } catch (err) {}

                                    await socket.sendMessage(sender, {
                                        document: { url: finalLinkObj.link },
                                        mimetype: 'video/mp4',
                                        fileName: `${tvInfo.title} - ${episode.episode_name || 'Episode ' + (i+1)}.mp4`,
                                        caption: `*📺 𝗦𝗛𝗔𝗚𝗚𝗬 𝗠𝗢𝗩𝗜𝗘 𝗕𝗢𝗧 📺*\\n\\n🎭 *Title:* ${tvInfo.title}\\n📌 *Episode:* ${episode.episode_name || 'Episode ' + (i+1)}\\n📊 *Quality:* Direct MP4\\n\\n${DEFAULT_FOOTER}`,
                                        jpegThumbnail: jpegThumbnail
                                    }, { quoted: replyMek });

                                    successCount++;
                                } else {
                                    failCount++;
                                }

                                await new Promise(resolve => setTimeout(resolve, 2500));

                            } catch (epError) {
                                console.error(`Error downloading episode:`, epError);
                                failCount++;
                            }
                        }

                        await socket.sendMessage(sender, { 
                            text: `*❪ SUMMARY ❫*\n\n🎉 *Download Complete!*\n\n🎬 *Series:* _${tvInfo.title}_\n✅ *Success:* _${successCount} Episodes_\n❌ *Failed:* _${failCount} Episodes_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });

                        socket.ev.off('messages.upsert', handleSelection);

                    } catch (tvShowError) {
                        console.error('TV Show error:', tvShowError);
                        await socket.sendMessage(sender, {
                            text: `*❪ ERROR ❫*\n\n❌ *TV Details Error!*\n🚫 _        ext ${tvShowError.message}_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }

                } else {
                    // MOVIE FLOW
                    await socket.sendMessage(sender, { 
                        text: `*❪ FETCHING ❫*\n\n🎬 *Fetching Movie details from ${site.toUpperCase()}...*\n⚡ _Please wait..._`
                    }, { quoted: replyMek });

                    try {
                        const detailsResponse = await axios.get(`${API_BASE}/api/v1/movie/${site}/infodl?q=        ext ${encodeURIComponent(selectedItem.link)}&api_key=${API_KEY}`);
                        const detailsData = detailsResponse.data;

                        if (!detailsData.status || !detailsData.data) {
                            throw new Error('Failed to fetch details');
                        }

                        const movieInfo = detailsData.data;
                        const validDownloads = movieInfo.downloads || [];

                        if (validDownloads.length === 0) {
                            await socket.sendMessage(sender, {
                                text: `*❪ NO DOWNLOADS ❫*\n\n⚠️ *No Downloads Found!*\n😞 _There are no downloads available for this movie!_${DEFAULT_FOOTER}`
                            }, { quoted: replyMek });
                            return;
                        }

                        const movieDetailsText = `*❪ MOVIE DETAILS ❫*\n\n🎬 *${movieInfo.title}*\n⭐ 𝗜𝗠𝗗𝗕 ➜ ★ ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n📅 𝗬𝗲𝗮𝗿 ➜         ext ${movieInfo.year || 'N/A'}\n⏳ 𝗗𝘂𝗿𝗮𝘁𝗶𝗼𝗻 ➜ ${movieInfo.duration || 'N/A'}\n🌍 🇨🇴🇺🇳🇹🇷🇾 ➜ ${movieInfo.country || 'N/A'}\n🎭 🇬𝗲𝗻𝗿𝗲𝘀 ➜ ${movieInfo.genres ? movieInfo.genres.join(', ') : 'N/A'}\n🏷️ 𝗟𝗮𝗻𝗴 ➜ ${movieInfo.language || movieInfo.tag || 'N/A'}\n🎬 𝗗𝗶𝗿𝗲𝗰𝘁𝗼𝗿 ➜ ${movieInfo.directors || movieInfo.director || 'N/A'}\n⭐ 𝗖𝗮𝘀𝘁 ➜ ${movieInfo.stars || 'N/A'}\n📝 𝗦𝘁𝗼𝗿𝘆 ➜ ${movieInfo.story ? (movieInfo.story.length > 250 ? movieInfo.story.substring(0, 250) + '...' : movieInfo.story) : 'N/A'}\n🗿 𝗦𝗼𝘂𝗿𝗰𝗲 ➜ ${site.toUpperCase()}\n ${DEFAULT_FOOTER}`;

                        const moviePosterUrl = movieInfo.image || selectedItem.image || DEFAULT_IMAGE;
                        await socket.sendMessage(sender, {
                            image: { url: moviePosterUrl },
                            caption: movieDetailsText
                        }, { quoted: replyMek });

                        const downloadOptionsText = `*❪ DOWNLOADS ❫*\n\n📥 *Select Quality:*\n\n${validDownloads.map((dl, i) => {
    const num = (i + 1) < 10 ? `0${i + 1}` : `${i + 1}`;
    const qualityIcon = (dl.quality || '').includes('1080') ? '🔥' : (dl.quality || '').includes('720') ? '💎' : '📱';
    return `*${num}* ➜ ${qualityIcon} _${dl.quality}_ 💾 _${dl.size || 'N/A'}_`;
}).join('\n')}\n\n*💬 REPLY TO DOWNLOAD 💬*\n📌 _Reply with the number_${DEFAULT_FOOTER}`;

                        const dlSentMsg = await socket.sendMessage(sender, { text: downloadOptionsText }, { quoted: replyMek });
                        const dlMessageID = dlSentMsg.key.id;

                        const handleDownloadSelection = async ({ messages: dlReplyMessages }) => {
                            const dlReplyMek = dlReplyMessages[0];
                            if (!dlReplyMek?.message) return;

                            const dlChoiceText = dlReplyMek.message.conversation || dlReplyMek.message.extendedTextMessage?.text;
                            const isReplyToDlMsg = dlReplyMek.message.extendedTextMessage?.contextInfo?.stanzaId === dlMessageID;

                            if (isReplyToDlMsg && sender === dlReplyMek.key.remoteJid) {
                                const dlChoice = parseInt(dlChoiceText) - 1;
                                if (isNaN(dlChoice) || dlChoice < 0 || dlChoice >= validDownloads.length) {
                                    await socket.sendMessage(sender, {
                                        text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 - ${validDownloads.length}_\n📝 _Please reply with a valid number!_${DEFAULT_FOOTER}`
                                    }, { quoted: dlReplyMek });
                                    return;
                                }

                                const selectedDownload = validDownloads[dlChoice];

                                await socket.sendMessage(sender, { 
                                    text: `*❪ SENDING MOVIE ❫*\n\n📥 *Sending:* _${movieInfo.title}_
📊 *Quality:* _${selectedDownload.quality}_
💾 *Size:* _${selectedDownload.size || 'N/A'}_
⚡ _Uploading file to WhatsApp..._`
                                }, { quoted: dlReplyMek });

                                try {
                                    let jpegThumbnail = undefined;
                                    try {
                                        const thumbRes = await axios.get(moviePosterUrl, { responseType: 'arraybuffer' });
                                        jpegThumbnail = Buffer.from(thumbRes.data).toString('base64');
                                    } catch (err) {}

                                    await socket.sendMessage(sender, {
                                        document: { url: selectedDownload.link },
                                        mimetype: 'video/mp4',
                                        fileName: `${movieInfo.title} (${selectedDownload.quality}).mp4`,
                                        caption: `*🎬 𝗦𝗛𝗔𝗚𝗚??  𝗠𝗢𝗩𝗜𝗘 𝗕𝗢𝗧 🎬*\\n\\n🎭 *Title:* ${movieInfo.title}\\n🌟 *IMDB:* ${movieInfo.imdb || movieInfo.rating || 'N/A'}\\n📅 *Year:* ${movieInfo.year || 'N/A'}\\n📊 *Quality:* ${selectedDownload.quality}\\n💾 *Size:* ${selectedDownload.size || 'N/A'}\\n\\n${DEFAULT_FOOTER}`,
                                        jpegThumbnail: jpegThumbnail
                                    }, { quoted: dlReplyMek });
                                } catch (uploadErr) {
                                    await socket.sendMessage(sender, {
                                        text: `*❪ UPLOAD FAILED ❫*\n\n❌ *Failed to upload file directly!*\n🔗 *Direct Link:* ${selectedDownload.link}${DEFAULT_FOOTER}`
                                    }, { quoted: dlReplyMek });
                                }

                                socket.ev.off('messages.upsert', handleDownloadSelection);
                            }
                        };

                        socket.ev.on('messages.upsert', handleDownloadSelection);
                        socket.ev.off('messages.upsert', handleSelection);

                    } catch (movieDetailsError) {
                        console.error('Movie Details error:', movieDetailsError);
                        await socket.sendMessage(sender, {
                            text: `*❪ ERROR ❫*\n\n❌ *Movie Details Error!*\n🚫 _${movieDetailsError.message}_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }
                }
            }
        };

        socket.ev.on('messages.upsert', handleSelection);

    } catch (error) {
        console.error('Unified Movie search error:', error);
        await socket.sendMessage(sender, {
            text: `*❪ SYSTEM ERROR ❫*\n\n❌ *System Error!*\n🚫 _${error.message || 'Unknown error'}_\n\n🔄 _Please try again later..._${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }

    break;
}    
case 'anime':
    if (!args.length) {
        await socket.sendMessage(sender, {
            image:  { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර ඇනිමේ එකේ නම ලබාදෙන්න! උදා: .anime naruto*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const animehave = args.join(' ');
    await socket.sendMessage(sender, { text: '🎬 𝙎𝙚𝙖𝙧𝙘𝙝𝙞𝙣𝙜 𝙖𝙣𝙞𝙢𝙚 𝙤𝙣 𝘼𝙣𝙞𝙢𝙚𝙃𝙚𝙖𝙫𝙚𝙣...' });


    await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));


    let animeSelectionListener = null;
    let animeEpisodeListener = null;
    let animeSelectionTimeout = null;
    let animeEpisodeTimeout = null;
    let animeMasterTimeout = null;
    const clearAllAnimeListeners = () => {



        if (animeSelectionListener) {
            socket.ev.off('messages.upsert', animeSelectionListener);
            animeSelectionListener = null;
        }
        if (animeSelectionTimeout) {
            clearTimeout(animeSelectionTimeout);
            animeSelectionTimeout = null;
        }


        if (animeEpisodeListener) {
            socket.ev.off('messages.upsert', animeEpisodeListener);
            animeEpisodeListener = null;
        }
        if (animeEpisodeTimeout) {
            clearTimeout(animeEpisodeTimeout);
            animeEpisodeTimeout = null;
        }


        if (animeMasterTimeout) {
            clearTimeout(animeMasterTimeout);
            animeMasterTimeout = null;
        }
    };

    try {

        const searchResponse = await axios.get(`${config.API_MAIN_URL}/animeheaven/search?query=${encodeURIComponent(animehave)}&api_key=${config.API_KEY}`);
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.data?.results || searchData.data.results.length === 0) {
            await socket.sendMessage(sender, {
                image:  { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*ඇනිමේ හමුවෙන්නේ නැත! 😞*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }


        const uniqueResults = [];
        const seenIds = new Set();
        for (const item of searchData.data.results) {
            if (!seenIds.has(item.anime_id)) {
                seenIds.add(item.anime_id);
                uniqueResults.push(item);
            }
        }

        const animeResults = uniqueResults.slice(0, 25);


        let listText = `☘️ *𝗔𝗡𝗜𝗠𝗘 𝗛𝗘𝗔𝗩𝗘𝗡 𝗦𝗘𝗔𝗥𝗖𝗛 : _${animehave}_*
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤\n*╭──────●➤*\n`;
        animeResults.forEach((item, index) => {
            listText += `*🤡 ${index + 1} ║❯❯ ${item.title}*\n`;
        });

        listText += `╰──────────●➤\n${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const bannerUrl = config.ANIME_H;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig)},
            caption: listText
        }, { quoted: msg });

        const messageID = sentMsg.key.id;


        animeMasterTimeout = setTimeout(() => {
            clearAllAnimeListeners();
            console.log('🧹 Anime master timeout - All listeners cleared after 3 minutes');
        }, 180000);


        const handleAnimeSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {

                if (animeSelectionTimeout) {
                    clearTimeout(animeSelectionTimeout);
                    animeSelectionTimeout = null;
                }


                animeSelectionTimeout = setTimeout(() => {
                    if (animeSelectionListener) {
                        socket.ev.off('messages.upsert', animeSelectionListener);
                        animeSelectionListener = null;
                        console.log('🧹 Anime selection listener timeout');
                    }
                    animeSelectionTimeout = null;
                }, 120000);

                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= animeResults.length) {
                    await socket.sendMessage(sender, {
                        image:  { url: pickBotImage(sessionConfig) },
                        caption: formatMessage(
                            '❌ INVALID SELECTION',
                            `*වැරදි අංකයක්! 1-${animeResults.length} අතර තෝරන්න! 😕*`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                    return;
                }

                const selectedItem = animeResults[choice];

                await socket.sendMessage(sender, { 
                    text: '📽️ 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙖𝙣𝙞𝙢𝙚 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' 
                }, { quoted: replyMek });


                await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

                try {

                    const detailsResponse = await axios.get(`${config.API_MAIN_URL}/animeheaven/info?url=${encodeURIComponent(selectedItem.url)}&api_key=${config.API_KEY}`);
                    const detailsData = detailsResponse.data;

                    if (!detailsData.status || !detailsData.anime) {
                        throw new Error('Failed to fetch anime details');
                    }

                    const animeInfo = detailsData.anime;

                    if (!animeInfo.episodeList || animeInfo.episodeList.length === 0) {
                        await socket.sendMessage(sender, {
                            image:  { url: pickBotImage(sessionConfig) },
                            caption: formatMessage(
                                '❌ NO EPISODES',
                                '*මෙම ඇනිමේ එක සඳහා කථාංග නොමැත!*',
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: replyMek });
                        return;
                    }


                    const animeTitle = animeInfo.title || selectedItem.title;
                    const japaneseTitle = animeInfo.japaneseTitle || '';
                    const description = animeInfo.description || 'No description available.';
                    const fullDescription = description.length > 400 ? description.substring(0, 400) + '...' : description;


                    const episodes = animeInfo.episodeList.sort((a, b) => a.episode - b.episode);
                    const totalEpisodes = animeInfo.episodes || episodes.length;


                    const tags = animeInfo.tags?.slice(0, 6).join(', ') || 'Anime';
                    const year = animeInfo.year || 'N/A';
                    const score = animeInfo.score || 'N/A';


                    const detailsCaption = `☘️ *${animeTitle}*
                    
▫️🇯🇵 *𝗧ɪᴛʟᴇ ➟ ${japaneseTitle}*
▫️⭐ *𝗦𝗰𝗼ʀᴇ / 𝗥ᴀᴛɪɴɢ ➟ ${score}/10*
▫️🎭 *𝗚ᴇɴʀᴇꜱ / 𝗧ᴀɢꜱ ➟ ${tags}*
▫️📅 *𝗥ᴇʟᴇᴀꜱᴇ 𝗬ᴇᴀʀ ➟ ${year}*
▫️🔢 *𝗧ᴏᴛᴀʟ 𝗘ᴘɪꜱᴏᴅᴇꜱ ➟ ${totalEpisodes}*
▫️📖 *Sᴛᴏʀʏ➟ ${fullDescription}*

> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

                    const posterUrl = animeInfo.poster || selectedItem.thumbnail || pickBotImage(sessionConfig);


                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: posterUrl },
                        caption: detailsCaption
                    }, { quoted: replyMek });


                    await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));


                    const displayEpisodes = episodes.slice(0, 999);

                    let episodeText = `*⬇️🍀 𝗘𝗣𝗜𝗦𝗢𝗗𝗘 𝗟𝗜𝗦𝗧 𝗢𝗣𝗧𝗜𝗢𝗡𝗦*
                    
_*Reply with NUMBER (1-${displayEpisodes.length}) to download single episode*_

*╭──────●➤*
${displayEpisodes.map((ep, idx) => {
    const episodeNum = ep.episode || idx + 1;
    const releasedDate = ep.releasedTime || '';
    return `🎀${idx + 1}┃➤ Episode ${episodeNum}${releasedDate ? ` ┃ ${releasedDate}` : ''}`;
}).join('\n')}
╰──────────●➤

${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

                    const episodeMsg = await socket.sendMessage(sender, {
                        text: episodeText
                    }, { quoted: infoMsg });

                    const episodeMsgID = episodeMsg.key.id;


                    const downloadSingleAnimeEpisode = async (episodeData, episodeMek) => {
                        try {
                            const episodeNumToShow = episodeData.episode || (episodes.findIndex(ep => ep === episodeData) + 1);

                            await socket.sendMessage(sender, { 
                                text: `⏳ 𝙂𝙚𝙩𝙩𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠 𝙛𝙤𝙧 𝙀𝙥𝙞𝙨𝙤𝙙𝙚 ${episodeNumToShow}...` 
                            }, { quoted: episodeMek });


                            await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

                            const downloadResponse = await axios.get(`${config.API_MAIN_URL}/animeheaven/get-link?gate_id=${episodeData.gateId}&api_key=${config.API_KEY}`);

                            let videoUrl = null;

                            if (downloadResponse.data && downloadResponse.data.status === true) {
                                videoUrl = downloadResponse.data.downloadLink;
                            }

                            if (!videoUrl && downloadResponse.data && downloadResponse.data.url) {
                                videoUrl = downloadResponse.data.url;
                            }

                            if (!videoUrl) {
                                throw new Error('Failed to get download URL from API response');
                            }

                            const videoConfig = {
                                url: videoUrl,
                                headers: {
                                    'Referer': 'https://animeheaven.me/'
                                }
                            };

                            const fileName = `${animeTitle} - Episode ${episodeNumToShow}.mp4`;

                            await socket.sendMessage(sender, {
                                document: { url: videoUrl, ...videoConfig },
                                mimetype: 'video/mp4',
                                fileName: fileName,
                                caption: formatMessage(
                                    `🍀 ${animeTitle}`,
                                    `📺 *Episode:* ${episodeNumToShow}`,
                                    `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                                )
                            }, { quoted: episodeMek });

                            await socket.sendMessage(sender, { react: { text: '✅', key: episodeMek.key } });


                            clearAllAnimeListeners();
                            return true;

                        } catch (error) {
                            console.error(`Download error for episode ${episodeData.episode}:`, error);
                            const episodeNumToShow = episodeData.episode || (episodes.findIndex(ep => ep === episodeData) + 1);
                            await socket.sendMessage(sender, {
                                image:  { url: pickBotImage(sessionConfig) },
                                caption: formatMessage(
                                    '❌ DOWNLOAD FAILED',
                                    `*Episode ${episodeNumToShow} download failed*\n${error.message}`,
                                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                )
                            }, { quoted: episodeMek });
                            return false;
                        }
                    };


                    const handleAnimeEpisode = async ({ messages: episodeMessages }) => {
                        const episodeMek = episodeMessages[0];
                        if (!episodeMek?.message) return;

                        const userInput = (episodeMek.message.conversation || episodeMek.message.extendedTextMessage?.text || '').trim().toLowerCase();
                        const isReplyToEpisodeMsg = episodeMek.message.extendedTextMessage?.contextInfo?.stanzaId === episodeMsgID;

                        if (isReplyToEpisodeMsg && sender === episodeMek.key.remoteJid) {

                            if (animeEpisodeTimeout) {
                                clearTimeout(animeEpisodeTimeout);
                                animeEpisodeTimeout = null;
                            }


                            animeEpisodeTimeout = setTimeout(() => {
                                if (animeEpisodeListener) {
                                    socket.ev.off('messages.upsert', animeEpisodeListener);
                                    animeEpisodeListener = null;
                                    console.log('🧹 Anime episode listener timeout');
                                }
                                animeEpisodeTimeout = null;
                            }, 120000);


                            const selectedIndex = parseInt(userInput) - 1;

                            if (isNaN(selectedIndex) || selectedIndex < 0 || selectedIndex >= episodes.length) {
                                await socket.sendMessage(sender, {
                                    image:  { url: pickBotImage(sessionConfig) },
                                    caption: formatMessage(
                                        '❌ INVALID INPUT',
                                        `*කරුණාකර වලංගු අංකයක් ඇතුළත් කරන්න! (1-${episodes.length})*\n\nඋදා: \`1\` හෝ \`5\``,
                                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                    )
                                }, { quoted: episodeMek });
                                return;
                            }

                            const episodeData = episodes[selectedIndex];

                            if (!episodeData) {
                                await socket.sendMessage(sender, {
                                    image:  { url: pickBotImage(sessionConfig) },
                                    caption: formatMessage(
                                        '❌ INVALID EPISODE',
                                        `*වැරදි අංකයක්! 1-${episodes.length} අතර තෝරන්න.*`,
                                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                    )
                                }, { quoted: episodeMek });
                                return;
                            }

                            await downloadSingleAnimeEpisode(episodeData, episodeMek);
                            socket.ev.off('messages.upsert', handleAnimeEpisode);
                            socket.ev.off('messages.upsert', handleAnimeSelection);
                        }
                    };


                    animeEpisodeListener = handleAnimeEpisode;
                    socket.ev.on('messages.upsert', handleAnimeEpisode);

                    animeEpisodeTimeout = setTimeout(() => {
                        if (animeEpisodeListener) {
                            socket.ev.off('messages.upsert', animeEpisodeListener);
                            animeEpisodeListener = null;

                        }
                        animeEpisodeTimeout = null;
                    }, 120000);

                } catch (detailsError) {
                    console.error('Anime details error:', detailsError);
                    await socket.sendMessage(sender, {
                        image:  { url: pickBotImage(sessionConfig) },
                        caption: formatMessage(
                            '❌ ERROR',
                            `*Details ලබාගැනීමේ දෝෂයක්*\n${detailsError.message}`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                }
            }
        };


        animeSelectionListener = handleAnimeSelection;
        socket.ev.on('messages.upsert', handleAnimeSelection);

        animeSelectionTimeout = setTimeout(() => {
            if (animeSelectionListener) {
                socket.ev.off('messages.upsert', animeSelectionListener);
                animeSelectionListener = null;
                console.log('🧹 Anime selection listener timeout');
            }
            animeSelectionTimeout = null;
        }, 120000);

    } catch (error) {
        console.error('Anime command error:', error);

        clearAllAnimeListeners();
        await socket.sendMessage(sender, {
            image:  { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                `*දෝෂයක් ඇතිවුණා:* ${error.message || 'Unknown error'}`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }
    break;

     // ==========================================

case 'schedule':
case 'remind': {
    if (!isOwner) {
        return await socket.sendMessage(sender, {
            text: "❌ *Only the bot owner can use this command.*"
        }, { quoted: msg });
    }

    const input = args.join(' ');
    const parts = input.split('|');

    if (parts.length < 3) {
        let helpText = `🎀 *𝗦𝗖𝗛𝗘𝗗𝗨𝗟𝗘𝗥  𝗠𝗔𝗡𝗔𝗚𝗘𝗥*\n\n` +
            `📝 *𝖴𝗌𝖺𝗀𝖾 :* \`.schedule NUMBER | MESSAGE | TIME\`\n` +
            `✨ *𝖤𝗑𝖺𝗆𝗉𝗅𝖾 :* \`.schedule 94768069800 | Hello Bro | 30s\`\n` +
            `🫧 *𝖦𝗋𝗈𝗎𝗉 𝖤𝗑 :* \`.schedule 1203630...g.us | Meeting start! | 5m\`\n\n` +
            `🐞 *Time Units :* \`s\` (seconds), \`m\` (minutes), \`h\` (hours)\n` +
            `⚠️ *Note :* Use \`|\` (pipe) to separate parts.`;

        return await socket.sendMessage(sender, {
            image: { url: config.BOT_IMAGE || config.ERROR },
            caption: formatMessage(
                `⏰ 𝗦𝗖𝗛𝗘𝗗𝗨𝗟𝗘  𝗖𝗢𝗠𝗠𝗔𝗡𝗗`,
                helpText,
                `${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`
            )
        }, { quoted: msg });
    }

    let target = parts[0].trim();
    let reminderMsg = parts[1].trim(); // මැසේජ් එක දැන් දෙවනියට තියෙන්නේ
    let timeArg = parts[2].trim();     // වෙලාව දැන් තුන්වනියට තියෙන්නේ

    // Number එකක් නම් JID එකකට හරවා ගැනීම
    if (!target.includes('@s.whatsapp.net') && !target.includes('@g.us')) {
        target = target.replace(/[^0-9]/g, '') + '@s.whatsapp.net';
    }

    // Time එක convert කරගැනීම (s, m, h)
    const unit = timeArg.slice(-1).toLowerCase();
    const value = parseInt(timeArg.slice(0, -1));

    if (isNaN(value) || value <= 0 || !['s', 'm', 'h'].includes(unit)) {
        return await socket.sendMessage(sender, {
            text: `❌ *Invalid time format!*\nUse like: \`30s\`, \`5m\`, or \`1h\` at the end.`
        }, { quoted: msg });
    }

    let delayMs = value * 1000;
    if (unit === 'm') delayMs = value * 60 * 1000;
    if (unit === 'h') delayMs = value * 60 * 60 * 1000;

    if (delayMs > 24 * 60 * 60 * 1000) {
        return await socket.sendMessage(sender, {
            text: `❌ *Time limit exceeded!* Maximum schedule time is 24 hours.`
        }, { quoted: msg });
    }

    await socket.sendMessage(sender, {
        text: `⏳ *Scheduled successfully!*\nTarget: \`${target}\`\nTime: *${timeArg}*`
    }, { quoted: msg });

    // නියමිත වෙලාව ආවම වෙනත් අමතර වැකි නැතුව අදාළ මැසේජ් එක විතරක් යැවීම
    setTimeout(async () => {
        try {
            await socket.sendMessage(target, {
                text: reminderMsg
            });
        } catch (err) {
            console.error("Schedule Send Error:", err);
        }
    }, delayMs);
}
break;


                    case 'ai':
case 'codex': {
    const query = args.join(' ');
    if (!query && !msg.hasMedia) {
        return await socket.sendMessage(sender, {
            text: `❌ *What do you want to ask Codex AI?*\n✨ *Example:* \`\`.ai Quantum computing kiyanne mokakda?\`\``
        }, { quoted: msg });
    }

    try {
        await socket.sendMessage(sender, { react: { text: "🤖", key: msg.key } });

        let imageUrl = null;
        let videoUrl = null;

        // Image / Media support (කොටස් වලට photo එකක් හෝ caption එකක් එක්ක photo එකක් එව්වොත් handle කරන්න)
        const quotedMessage = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
        const isQuotedImage = quotedMessage?.imageMessage;
        const isDirectImage = msg.message?.imageMessage;

        if (isDirectImage || isQuotedImage) {
            // Media download කිරීම සඳහා Baileys වල downloadMediaMessage පාවිච්චි කරයි
            const stream = await downloadMediaMessage(
                isDirectImage ? msg : { message: quotedMessage },
                'buffer',
                {},
                { logger: console }
            );

            const mimeType = isDirectImage ? msg.message.imageMessage.mimetype : quotedMessage.imageMessage.mimetype;
            imageUrl = `data:${mimeType};base64,${stream.toString('base64')}`;
        }

        const CODEX_API_KEY = "cx_live_555l2y4l5a5t0y5z1x5a1i4j221o5h3j";
        const CODEX_URL = "https://code-x-ai.lovable.app/api/public/v1/chat";

        const apiResponse = await fetch(CODEX_URL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${CODEX_API_KEY}`
            },
            body: JSON.stringify({
                message: query || "Meke thiyenne mokakda?",
                session: sender,          // chat id එකම session එක විදිහට දීලා long-term memory active කිරීම
                image_url: imageUrl       // Vision / Photo support එක
            })
        });

        const resData = await apiResponse.json();
        const aiReply = resData.reply || resData.error || "Sorry, I couldn't process that.";

        await socket.sendMessage(sender, {
            text: `${aiReply}\n\n> ${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: "✨", key: msg.key } });

    } catch (err) {
        console.error("Codex AI Error:", err);
        await socket.sendMessage(sender, { text: `❌ *Codex AI service is currently busy.*` }, { quoted: msg });
    }
}
break;

   // ==========================================
// 1. SYSTEM / PING COMMAND
// ==========================================
case 'system':
case 'ping':
case 'status': {
    try {
        await socket.sendMessage(sender, { react: { text: "⚡", key: msg.key } });

        const os = await import('os');
        const startTime = process.hrtime();
        const diff = process.hrtime(startTime);
        const latency = (diff[0] * 1e3 + diff[1] * 1e-6).toFixed(4);

        const totalMem = os.totalmem();
        const freeMem = os.freemem();
        const usedMem = totalMem - freeMem;

        function formatBytes(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }

        const uptimeSeconds = process.uptime();
        const days = Math.floor(uptimeSeconds / (3600 * 24));
        const hours = Math.floor((uptimeSeconds % (3600 * 24)) / 3600);
        const minutes = Math.floor((uptimeSeconds % 3600) / 60);
        const seconds = Math.floor(uptimeSeconds % 60);
        const uptimeFormatted = `${days}d ${hours}h ${minutes}m ${seconds}s`;

        let systemText = `🖥️ *𝗦𝗛𝗔𝗚𝗚𝗬  𝗫𝗠𝗗  -  𝗦𝗬𝗦𝗧𝗘𝗠  𝗦𝗧𝗔𝗧𝗨𝗦* 📊\n\n` +
            `⚡ *𝖲ᵵᵃᵗᵘˢ 𝖲ᵖᵉᵉᵈ :* \`${latency} ms\`\n` +
            `⏳ *𝖴ᵖᵗⁱᵐᵉ :* \`${uptimeFormatted}\`\n` +
            `🧠 *𝖱𝖠𝖬 𝖴𝗌𝖺𝗀𝖾 :* \`${formatBytes(usedMem)} / ${formatBytes(totalMem)}\`\n` +
            `🌐 *𝖯𝖑ᵃᵗᶠᵒʳᵐ :* \`${os.platform()} (${os.arch})\`\n\n` +
            `> ${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`;

        await socket.sendMessage(sender, { text: systemText }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: "✅", key: msg.key } });
    } catch (err) {
        console.error("System Cmd Error:", err);
        await socket.sendMessage(sender, { text: `❌ *Failed to fetch system status.*` }, { quoted: msg });
    }
}
break;
// ==========================================
// LITEAPKS - MOD APK DOWNLOADER (FIXED)
// ==========================================
case 'liteapks':
case 'apk':
case 'mod': {
    const chatJid = msg.key.remoteJid;
    const DEFAULT_FOOTER = `\n\n> 📱 𝗦𝗛𝗔??𝗚𝗬 𝗫𝗠𝗗 📱\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ 👑 𝗦𝗛𝗔𝗚𝗚𝗬 𝗧𝗘𝗖𝗛`;

    const API_BASE = "https://api.chamindu.site";
    const API_KEY = "chama_api_11230a80e5eed3c1b80bfcc5d1773ec9";
    const DEFAULT_IMAGE = "https://liteapks.com/wp-content/uploads/2022/04/spotify-music-and-podcasts-150x150.png";
    const TEMP_DIR = './tmp_apk';

    function getCircledNumber(num) {
        const arr = ['①','②','③','④','⑤','⑥','⑦','⑧','⑨','⑩','⑪','⑫','⑬','⑭','⑮','⑯','⑰','⑱','⑲','⑳'];
        return arr[num - 1] || `[${num}]`;
    }

    // ═══ VALIDATION ═══
    if (!args.length) {
        return await socket.sendMessage(chatJid, {
            text: `*❪ LITEAPKS ❫*\n\n⚠️ *Usage:*\n• \`.apk spotify\`\n• \`.apk whatsapp\`\n• \`.apk instagram\`\n\n📌 _Premium APKs download කරන්න._${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }

    const apkQuery = args.join(' ');

    await socket.sendMessage(chatJid, {
        text: `*❪ SEARCHING ❫*\n\n🔍 *LiteAPKs හි සොයමින්...*\n⚡ _Please wait._`
    }, { quoted: msg });

    try {
        // ═══ STEP 1 : SEARCH ═══
        const searchRes = await axios.get(`${API_BASE}/api/v1/apk/liteapks/search`, {
            params: { q: apkQuery, api_key: API_KEY },
            timeout: 30000
        });

        const searchData = searchRes.data;
        const results = searchData.data || [];

        if (!searchData.status || results.length === 0) {
            return await socket.sendMessage(chatJid, {
                text: `*❪ NO RESULTS ❫*\n\n😞 *"${apkQuery}"* හමු නොවීය!${DEFAULT_FOOTER}`
            }, { quoted: msg });
        }

        const apkList = results.slice(0, 20);

        let listText = `📱 *𝗟𝗜𝗧𝗘𝗔𝗣𝗞𝗦 𝗦𝗘𝗔𝗥𝗖𝗛 : _${apkQuery}_*\n╭──────●➤\n*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        apkList.forEach((item, i) => {
            const num = getCircledNumber(i + 1);
            const badge = item.badge ? ` [${item.badge}]` : '';
            listText += `*${num} ➜ 📦 ${item.title}${badge}*\n   ↳ _${item.version || 'N/A'} | ${item.size || 'N/A'}_\n\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`;

        const sentMsg = await socket.sendMessage(chatJid, {
            image: { url: apkList[0].image || DEFAULT_IMAGE },
            caption: listText
        }, { quoted: msg });

        const messageID = sentMsg.key.id;
        const originalSender = (msg.key.participant || msg.key.remoteJid || '').split('@')[0].split(':')[0];

        // ═══ STEP 2 : USER PICKS ═══
        let cleanupTimeout = null;

        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages?.[0];
            if (!replyMek?.message) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;
            const replier = (replyMek.key.participant || replyMek.key.remoteJid || '').split('@')[0].split(':')[0];

            if (isReply && replier === originalSender) {
                if (cleanupTimeout) clearTimeout(cleanupTimeout);
                socket.ev.off('messages.upsert', handleSelection);

                const choice = parseInt(text) - 1;
                if (isNaN(choice) || choice < 0 || choice >= apkList.length) {
                    return socket.sendMessage(chatJid, {
                        text: `❌ *Invalid number!* Use 1 - ${apkList.length}`
                    }, { quoted: replyMek });
                }

                const selected = apkList[choice];

                await socket.sendMessage(chatJid, {
                    text: `⏳ *Fetching download link...*\n\n📦 *${selected.title}*\n📌 ${selected.version} | ${selected.size}`
                }, { quoted: replyMek });

                try {
                    // ═══ STEP 3 : GET DOWNLOAD LINK ═══
                    const dlRes = await axios.get(`${API_BASE}/api/v1/apk/liteapks/download`, {
                        params: { url: selected.link, api_key: API_KEY },
                        timeout: 30000
                    });

                    const dlData = dlRes.data;
                    if (!dlData.status || !dlData.url) {
                        throw new Error('Download link හමු නොවීය.');
                    }

                    const apkUrl = dlData.url;

                    // ═══ STEP 4 : DOWNLOAD & SEND ═══
                    await socket.sendMessage(chatJid, { react: { text: '📥', key: replyMek.key } });

                    await socket.sendMessage(chatJid, {
                        text: `⏳ *Downloading to server:* ${selected.title}\n📦 *Size:* ${selected.size}\n\n_කරුණාකර රැඳී සිටින්න..._`
                    }, { quoted: replyMek });

                    // Temp folder
                    await fs.ensureDir(TEMP_DIR);
                    const safeName = selected.title.replace(/[^a-zA-Z0-9]/g, '_').substring(0, 40);
                    const localFile = path.join(TEMP_DIR, `${safeName}_${Date.now()}.apk`);

                    try {
                        // ── 4a. Download to server ──
                        const writer = fs.createWriteStream(localFile);
                        const response = await axios({
                            url: apkUrl,
                            method: 'GET',
                            responseType: 'stream',
                            timeout: 0,
                            maxContentLength: Infinity,
                            maxBodyLength: Infinity,
                            headers: {
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                                'Accept': '*/*',
                                'Referer': 'https://liteapks.com/'
                            }
                        });
                        response.data.pipe(writer);
                        await new Promise((resolve, reject) => {
                            writer.on('finish', resolve);
                            writer.on('error', reject);
                        });

                        const stats = await fs.stat(localFile);
                        const realSizeMB = stats.size / 1024 / 1024;

                        await socket.sendMessage(chatJid, {
                            text: `✅ *Downloaded!*\n📦 ${realSizeMB.toFixed(1)} MB\n\n📤 _Sending to WhatsApp..._`
                        }, { quoted: replyMek });

                        // ── 4b. Send as document ──
                        const fileName = `${selected.title.replace(/[^a-zA-Z0-9 ]/g, '').trim()} ${selected.version}.apk`;

                        try {
                            await socket.sendMessage(chatJid, {
                                document: { url: localFile },
                                mimetype: 'application/octet-stream',      // ✅ generic binary
                                fileName: fileName,
                                caption: `✅ *${selected.title}*\n\n📌 *Version:* ${selected.version}\n📦 *Size:* ${selected.size}\n🏷️ *Badge:* ${selected.badge || 'MOD'}\n\n⚠️ *Install:* Settings → Security → Allow unknown sources${DEFAULT_FOOTER}`
                            }, { quoted: replyMek });

                            await socket.sendMessage(chatJid, { react: { text: '✅', key: replyMek.key } });

                        } catch (sendErr) {
                            // Send fail → link only
                            await socket.sendMessage(chatJid, {
                                text: `📦 *${selected.title}*\n\n📌 *Version:* ${selected.version}\n📦 *Size:* ${selected.size}\n\n⚠️ *File එක යැවිය නොහැක.*\n\n🔗 *Direct Download:*\n${apkUrl}${DEFAULT_FOOTER}`
                            }, { quoted: replyMek });
                        }

                        // ── 4c. Cleanup ──
                        await fs.remove(localFile).catch(() => {});

                    } catch (downloadErr) {
                        console.error('APK download error:', downloadErr.message);
                        await socket.sendMessage(chatJid, {
                            text: `❌ *Download fail:* _${downloadErr.message}_\n\n🔗 *Direct Link:*\n${apkUrl}`
                        }, { quoted: replyMek });

                        try { await fs.remove(localFile); } catch {}
                    }

                } catch (err) {
                    await socket.sendMessage(chatJid, {
                        text: `❌ *Error:* _${err.message}_`
                    }, { quoted: replyMek });
                }
            }
        };

        socket.ev.on('messages.upsert', handleSelection);

        cleanupTimeout = setTimeout(() => {
            socket.ev.off('messages.upsert', handleSelection);
        }, 180000);

    } catch (err) {
        console.error('LiteAPKs error:', err);
        await socket.sendMessage(chatJid, {
            text: `❌ *Error:* _${err.message}_${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }
    break;
}
// ==========================================
// AUTO REPLY COMMANDS
// ==========================================
case 'adauto':
case 'addauto': {
    const ADMIN_NUMBERS = (process.env.ADMIN_NUMBERS || '').split(',').map(n => n.trim()).filter(Boolean);
    if (!isOwner && !ADMIN_NUMBERS.includes(senderNumber)) {
        return await socket.sendMessage(sender, {
            text: "❌ *Admin only!*"
        }, { quoted: msg });
    }
    
    if (!args.length) {
        return await socket.sendMessage(sender, {
            text: `❌ *Usage:* \`.adauto keyword , message\`\n\n*Example:*\n\`.adauto hi , Hello! 👋\`\n\n💡 _Image එකකුත් දාන්න ඕන නම් settings web panel එකෙන් auto-reply add කරන්න - ඒකෙන් image URL එකකුත් දාන්න පුළුවන්._`
        }, { quoted: msg });
    }
    
    const fullText = args.join(' ');
    const commaIndex = fullText.indexOf(',');
    
    if (commaIndex === -1) {
        return await socket.sendMessage(sender, {
            text: `❌ *Wrong format!*\n\n*Usage:* \`.adauto keyword , message\`\n\n*Example:*\n\`.adauto hi , Hello! 👋\``
        }, { quoted: msg });
    }
    
    const keyword = fullText.substring(0, commaIndex).trim().toLowerCase();
    const replyText = fullText.substring(commaIndex + 1).trim();
    
    if (!keyword || !replyText) {
        return await socket.sendMessage(sender, {
            text: `❌ *Keyword සහ reply දෙකම දෙන්න!*`
        }, { quoted: msg });
    }
    
    try {
        await AutoReply.findOneAndUpdate(
            { number: sanitizedNumber, keyword },
            { number: sanitizedNumber, keyword, reply: replyText },
            { upsert: true }
        );
        await loadAutoReplyCache(sanitizedNumber); // 🆕 instant cache refresh
        
        await socket.sendMessage(sender, {
            text: `✅ *Auto-reply Added!*\n\n🔑 *Keyword:* \`${keyword}\`\n💬 *Reply:* _${replyText}_\n\n💡 _User "${keyword}" type කරාම bot reply එක යවනවා._`
        }, { quoted: msg });
        
    } catch (error) {
        await socket.sendMessage(sender, {
            text: `❌ Error: _${error.message}_`
        }, { quoted: msg });
    }
    break;
}

case 'delauto':
case 'removeauto': {
    const ADMIN_NUMBERS = (process.env.ADMIN_NUMBERS || '').split(',').map(n => n.trim()).filter(Boolean);
    if (!isOwner && !ADMIN_NUMBERS.includes(senderNumber)) {
        return await socket.sendMessage(sender, {
            text: "❌ *Admin only!*"
        }, { quoted: msg });
    }
    
    if (!args.length) {
        return await socket.sendMessage(sender, {
            text: `❌ *Usage:* \`.delauto keyword\`\n\n*Example:*\n\`.delauto hi\``
        }, { quoted: msg });
    }
    
    const keyword = args.join(' ').trim().toLowerCase();
    
    try {
        const result = await AutoReply.deleteOne({ number: sanitizedNumber, keyword });
        await loadAutoReplyCache(sanitizedNumber); // 🆕 instant cache refresh
        
        if (result.deletedCount === 0) {
            return await socket.sendMessage(sender, {
                text: `❌ \`${keyword}\` කියන auto-reply එකක් හමු නොවීය.`
            }, { quoted: msg });
        }
        
        await socket.sendMessage(sender, {
            text: `✅ *Auto-reply Removed!*\n\n🔑 \`${keyword}\``
        }, { quoted: msg });
        
    } catch (error) {
        await socket.sendMessage(sender, {
            text: `❌ Error: _${error.message}_`
        }, { quoted: msg });
    }
    break;
}

case 'autorep':
case 'ar':
case 'auto': {
    const ADMIN_NUMBERS = (process.env.ADMIN_NUMBERS || '').split(',').map(n => n.trim()).filter(Boolean);
    if (!isOwner && !ADMIN_NUMBERS.includes(senderNumber)) {
        return await socket.sendMessage(sender, {
            text: "❌ *Admin only!*"
        }, { quoted: msg });
    }
    
    const action = args[0]?.toLowerCase();
    
    // ─── REMOVE via autorep ───
    if (action === 'remove' || action === 'del') {
        const keyword = args.slice(1).join(' ').trim().toLowerCase();
        if (!keyword) {
            return await socket.sendMessage(sender, { text: `❌ Usage: \`.autorep remove keyword\`` }, { quoted: msg });
        }
        const result = await AutoReply.deleteOne({ number: sanitizedNumber, keyword });
        await loadAutoReplyCache(sanitizedNumber); // 🆕 instant cache refresh
        return await socket.sendMessage(sender, {
            text: result.deletedCount > 0 ? `✅ *Removed:* \`${keyword}\`` : `❌ \`${keyword}\` හමු නොවීය.`
        }, { quoted: msg });
    }
    
    // ─── LIST ───
    if (action === 'list' || !args.length) {
        const replies = await AutoReply.find({ number: sanitizedNumber }).lean();
        
        let text = `💬 *AUTO REPLY MANAGER*\n\n`;
        text += `📊 *Total:* ${replies.length}\n\n`;
        text += `*Commands:*\n`;
        text += `• \`.adauto keyword , message\` — Add\n`;
        text += `• \`.delauto keyword\` — Remove\n`;
        text += `• \`.autorep list\` — List\n`;
        text += `• \`.autorep clear\` — Clear all\n`;
        text += `• _Image ekakuth danna one nam settings web panel eken add karanna_\n\n`;
        
        if (replies.length > 0) {
            text += `━━━━━━━━━━━━━━━\n`;
            text += `*📋 SAVED REPLIES:*\n\n`;
            replies.slice(0, 20).forEach((ar, i) => {
                const preview = (ar.reply || '').length > 30 ? ar.reply.substring(0, 30) + '...' : (ar.reply || '');
                const imgTag = ar.image ? ' 🖼️' : '';
                text += `*${i + 1}.* \`${ar.keyword}\`${imgTag}\n→ ${preview}\n\n`;
            });
            if (replies.length > 20) {
                text += `_...and ${replies.length - 20} more_\n`;
            }
        }
        
        return await socket.sendMessage(sender, { text }, { quoted: msg });
    }
    
    // ─── CLEAR ───
    if (action === 'clear') {
        const result = await AutoReply.deleteMany({ number: sanitizedNumber });
        await loadAutoReplyCache(sanitizedNumber); // 🆕 instant cache refresh
        return await socket.sendMessage(sender, {
            text: `🗑️ Cleared *${result.deletedCount}* auto-replies.`
        }, { quoted: msg });
    }
    
    // ─── ADD via autorep ───
    const fullText = args.join(' ');
    const commaIndex = fullText.indexOf(',');
    
    if (commaIndex === -1) {
        return await socket.sendMessage(sender, {
            text: `❌ *Wrong format!*\n\nUse \`.adauto keyword , message\``
        }, { quoted: msg });
    }
    
    const keyword = fullText.substring(0, commaIndex).trim().toLowerCase();
    const replyText = fullText.substring(commaIndex + 1).trim();
    
    try {
        await AutoReply.findOneAndUpdate(
            { number: sanitizedNumber, keyword },
            { number: sanitizedNumber, keyword, reply: replyText },
            { upsert: true }
        );
        await loadAutoReplyCache(sanitizedNumber); // 🆕 instant cache refresh
        await socket.sendMessage(sender, {
            text: `✅ *Auto-reply Added!*\n\n🔑 \`${keyword}\`\n💬 _${replyText}_`
        }, { quoted: msg });
    } catch (error) {
        await socket.sendMessage(sender, {
            text: `❌ Error: _${error.message}_`
        }, { quoted: msg });
    }
    break;
}
// ==========================================
// 2. BOTS / SESSIONS COMMAND
// ==========================================
case 'sessions':
case 'connectedbots':
case 'bots': {
    if (!isOwner) {
        return await socket.sendMessage(sender, {
            text: "❌ *Only the bot owner can use this command.*"
        }, { quoted: msg });
    }

    try {
        await socket.sendMessage(sender, { react: { text: "🔍", key: msg.key } });

        const mongoose = (await import('mongoose')).default;
        const db = mongoose.connection.db;

        if (!db) {
            return await socket.sendMessage(sender, { text: `❌ *MongoDB connection is not active!*` }, { quoted: msg });
        }

        const collections = await db.listCollections().toArray();
        let sessionData = [];
        let foundCollectionName = '';

        const targetColl = collections.find(c => 
            c.name.toLowerCase().includes('session') || 
            c.name.toLowerCase().includes('auth') || 
            c.name.toLowerCase().includes('bot') ||
            c.name.toLowerCase().includes('baileys')
        );

        if (targetColl) {
            foundCollectionName = targetColl.name;
            const collection = db.collection(foundCollectionName);
            sessionData = await collection.find({}).limit(15).toArray();
        }

        let sessionText = `🤖 *𝗦𝗛𝗔𝗚𝗚𝗬  𝗫𝗠𝗗  -  𝗖𝗢𝗡𝗡𝗘𝗖𝗧𝗘𝗗  𝗕𝗢𝗧𝗦 / 𝗦𝗘𝗦𝗦𝗜𝗢𝗡𝗦* 🌐\n\n` +
            `📂 *Collection :* \`${foundCollectionName || 'None'}\`\n` +
            `📊 *Active Count :* \`${sessionData.length} Records\`\n\n`;

        if (sessionData.length > 0) {
            sessionData.forEach((ses, index) => {
                const num = index + 1;
                const idStr = JSON.stringify(ses._id || ses.id || 'Unknown');
                sessionText += `*${num}.* \`${idStr.replace(/["']/g, '')}\`\n`;
            });
        } else {
            sessionText += `_No active session keys found in MongoDB collections._\n`;
        }

        sessionText += `\n> ${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`;

        await socket.sendMessage(sender, { text: sessionText }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: "✅", key: msg.key } });

    } catch (err) {
        console.error("Sessions Cmd Error:", err);
        await socket.sendMessage(sender, { text: `❌ *Failed to fetch connected bots: ${err.message}*` }, { quoted: msg });
    }
}
break;
case 'cartoon':
case 'cartoonlk': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර කාටූනයේ නම ලබාදෙන්න! උදා: .cartoon Ben 10*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const cartoonQuery = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/cartoons/sinhalacartoons';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    let cartoonSelectionListener = null;
    let cartoonEpisodeListener = null;
    let cartoonMasterTimeout = null;

    const clearAllCartoonListeners = () => {
        if (cartoonSelectionListener) {
            socket.ev.off('messages.upsert', cartoonSelectionListener);
            cartoonSelectionListener = null;
        }
        if (cartoonEpisodeListener) {
            socket.ev.off('messages.upsert', cartoonEpisodeListener);
            cartoonEpisodeListener = null;
        }
        if (cartoonMasterTimeout) {
            clearTimeout(cartoonMasterTimeout);
            cartoonMasterTimeout = null;
        }
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching cartoons on SinhalaCartoons...' }, { quoted: msg });

        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: cartoonQuery, api_key: API_KEY },
            timeout: 20000
        });

        const searchData = searchRes.data;
        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: pickBotImage(sessionConfig) },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*කිසිදු කාටූනයක් හමු නොවීය!*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const cartoonList = searchData.data.slice(0, 20);
        let listText = `🧸 *𝗦𝗜𝗡𝗛𝗔𝗟𝗔 𝗖𝗔𝗥𝗧𝗢𝗢𝗡 𝗦𝗘𝗔𝗥𝗖𝗛 : _${cartoonQuery}_*\n╭──────●➤\n*🔢 ʀᴇ𝗽𝗹ʏ ʙᴇʟ𝗼ᴡ ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        cartoonList.forEach((item, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${item.title}*\n    ↳ (${item.quality || 'HD'} | ⭐ ${item.rating || 'N/A'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: cartoonList[0].image || pickBotImage(sessionConfig) },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        cartoonMasterTimeout = setTimeout(() => {
            clearAllCartoonListeners();
        }, 120000);

        const handleCartoonSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;

            if (isReply) {
                const choice = parseInt(text) - 1;
                if (isNaN(choice) || choice < 0 || choice >= cartoonList.length) {
                    await socket.sendMessage(sender, {
                        text: `❌ කරුණාකර 1 - ${cartoonList.length} අතර අංකයක් ලබාදෙන්න!`
                    }, { quoted: replyMek });
                    return;
                }

                if (cartoonSelectionListener) {
                    socket.ev.off('messages.upsert', cartoonSelectionListener);
                    cartoonSelectionListener = null;
                }

                const chosenCartoon = cartoonList[choice];
                await socket.sendMessage(sender, { text: '⏳ Fetching cartoon details & episodes...' }, { quoted: replyMek });

                try {
                    const infoRes = await axios.get(`${API_BASE}/infodl`, {
                        params: { q: chosenCartoon.link, api_key: API_KEY },
                        timeout: 20000
                    });

                    const cartoonData = infoRes.data?.data;
                    const allDownloads = cartoonData?.downloads || [];

                    if (!cartoonData || allDownloads.length === 0) {
                        throw new Error('බාගත කිරීමේ links හෝ episodes හමු නොවීය.');
                    }

                    const directDownloads = allDownloads.filter(d => d.link?.endsWith('.mp4') || !d.name?.includes('Telegram'));
                    const finalDownloads = directDownloads.length > 0 ? directDownloads : allDownloads;

                    let infoText = `🍀 *${cartoonData.title}*\n\n`;
                    infoText += `⭐ *IMDb:* ${cartoonData.imdb || 'N/A'}\n`;
                    infoText += `🗣️ *Language:* ${cartoonData.language || 'Sinhala'}\n`;
                    infoText += `🎭 *Genres:* ${cartoonData.genres?.join(', ') || 'Cartoon'}\n\n`;
                    infoText += `*Available Episodes / Links:*\n`;

                    finalDownloads.forEach((dl, i) => {
                        infoText += `*${i + 1}.* ${dl.name}\n`;
                    });
                    infoText += `\n👉 *බාගත කිරීමට අදාළ Episode අංකය Reply කරන්න.*`;

                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: cartoonData.image || chosenCartoon.image },
                        caption: infoText
                    }, { quoted: replyMek });

                    const infoMsgID = infoMsg.key.id;

                    const handleEpisodeSelection = async ({ messages: epMessages }) => {
                        const epMek = epMessages?.[0];
                        if (!epMek?.message || epMek.key.remoteJid !== sender) return;

                        const epChoiceText = (epMek.message.conversation || epMek.message.extendedTextMessage?.text || '').trim();
                        const isEpReply = epMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;

                        if (isEpReply) {
                            const epIdx = parseInt(epChoiceText) - 1;
                            if (isNaN(epIdx) || epIdx < 0 || epIdx >= finalDownloads.length) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ කරුණාකර 1 - ${finalDownloads.length} අතර Episode අංකයක් ලබාදෙන්න!` 
                                }, { quoted: epMek });
                                return;
                            }

                            clearAllCartoonListeners();
                            const selectedEpisode = finalDownloads[epIdx];

                            await socket.sendMessage(sender, { react: { text: '📥', key: epMek.key } });

                            await socket.sendMessage(sender, { 
                                text: `⏳ *Downloading Episode:* ${selectedEpisode.name}\n_කරුණාකර ටික වේලාවක් රැඳී සිටින්න, වීඩියෝව ඩවුන්ලෝඩ් වෙමින් පවතී..._` 
                            }, { quoted: epMek });

                            try {
                                // Direct Link එක වෙනුවට Document MP4 එකක් ලෙස යැවීම
                                await socket.sendMessage(sender, {
                                    document: { url: selectedEpisode.link },
                                    mimetype: 'video/mp4',
                                    fileName: `${cartoonData.title} - ${selectedEpisode.name}.mp4`,
                                    caption: `✅ *CARTOON DOWNLOADED*\n\n🎬 *Series:* ${cartoonData.title}\n📌 *Episode:* ${selectedEpisode.name}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                }, { quoted: epMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: epMek.key } });
                            } catch (uploadErr) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ වීඩියෝව යැවීමේදී දෝෂයක් ඇති විය: ${uploadErr.message}\n\n🔗 Direct Link එක: ${selectedEpisode.link}` 
                                }, { quoted: epMek });
                            }
                        }
                    };

                    cartoonEpisodeListener = handleEpisodeSelection;
                    socket.ev.on('messages.upsert', handleEpisodeSelection);

                } catch (infoErr) {
                    clearAllCartoonListeners();
                    await socket.sendMessage(sender, { text: `❌ Cartoon Info Error: ${infoErr.message}` }, { quoted: replyMek });
                }
            }
        };

        cartoonSelectionListener = handleCartoonSelection;
        socket.ev.on('messages.upsert', handleCartoonSelection);

    } catch (err) {
        clearAllCartoonListeners();
        await socket.sendMessage(sender, {
            text: `❌ Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
}
case 'news':
    case 'siyatha': {
        try {
            const apiUrl = 'https://api-siteh-22e22e4cb068.herokuapp.com/news/siyatha?api_key=lakiya_2f3b6c382d1236ad7a08d56331fb679935d51dfc846df2c254093fd1fff9494e';
            const response = await axios.get(apiUrl);
            const resData = response.data;

            if (resData.status && resData.result) {
                let newsItem = resData.result;
                let caption = `📰 *${newsItem.title}*\n\n` +
                              `📅 *Date:* ${newsItem.date}\n\n` +
                              `${newsItem.desc}\n\n` +
                              `🔗 *Link:* ${newsItem.link}`;

                await socket.sendMessage(from, { 
                    image: { url: newsItem.image }, 
                    caption: caption 
                }, { quoted: msg });
            } else {
                await socket.sendMessage(from, { text: '❌ පුවත් ලබාගැනීමේදී දෝෂයක් ඇති විය.' }, { quoted: msg });
            }
        } catch (e) {
            console.error(e);
            await socket.sendMessage(from, { text: '❌ දෝෂයක් සිදු විය: ' + e.message }, { quoted: msg });
        }
        break;
    }

    case 'fitgirl':
    case 'fg': {
        const q = args.join(' ').trim();
        try {
            if (!q) return await socket.sendMessage(from, { text: '❌ කරුණාකර සෙවිය යුතු ක්‍රීඩාවේ නමක් සඳහන් කරන්න!\nඋදා: `.fitgirl far cry`' }, { quoted: msg });

            const searchUrl = `https://api-siteh-22e22e4cb068.herokuapp.com/fitgirl/search?game=${encodeURIComponent(q)}`;
            const response = await axios.get(searchUrl);
            const resData = response.data;

            if (resData.status && resData.results && resData.results.length > 0) {
                let txt = "🎮 *FitGirl Repacks Search Results* 🎮\n\n";
                resData.results.forEach((game, index) => {
                    txt += "*" + (index + 1) + ".* " + game.title + "\n🔗 " + game.link + "\n\n";
                });
                txt += "*සම්පූර්ණ විස්තර බැලීමට .fginfo [game name] භාවිතා කරන්න.*";

                await socket.sendMessage(from, { text: txt }, { quoted: msg });
            } else {
                await socket.sendMessage(from, { text: '❌ ප්‍රතිඵල හමු නොවීය.' }, { quoted: msg });
            }
        } catch (e) {
            console.error(e);
            await socket.sendMessage(from, { text: '❌ දෝෂයක් සිදු විය: ' + e.message }, { quoted: msg });
        }
        break;
    }

    case 'fginfo':
    case 'fitgirlinfo': {
        const q = args.join(' ').trim();
        try {
            if (!q) return await socket.sendMessage(from, { text: '❌ කරුණාකර game එකේ නම නිවැරදිව ලබා දෙන්න!\nඋදා: `.fginfo far cry 5`' }, { quoted: msg });

            const infoUrl = `https://api-siteh-22e22e4cb068.herokuapp.com/fitgirl/complete?game=${encodeURIComponent(q)}`;
            const response = await axios.get(infoUrl);
            const resData = response.data;

            if (resData.status && resData.data && resData.data.game) {
                let g = resData.data.game;
                let caption = `🎮 *${g.title}*\n\n` +
                              `📌 *Version:* ${g.version}\n` +
                              `🏢 *Companies:* ${g.companies}\n` +
                              `🌐 *Languages:* ${g.languages}\n` +
                              `📦 *Original Size:* ${g.original_size}\n` +
                              `💾 *Repack Size:* ${g.repack_size}\n` +
                              `🏷️ *Categories:* ${g.categories.join(', ')}\n` +
                              `📅 *Published:* ${g.published_date}`;

                await socket.sendMessage(from, { 
                    image: { url: g.poster }, 
                    caption: caption 
                }, { quoted: msg });
            } else {
                await socket.sendMessage(from, { text: '❌ අදාළ ක්‍රීඩාවේ තොරතුරු ලබා ගැනීමට නොහැකි විය.' }, { quoted: msg });
            }
        } catch (e) {
            console.error(e);
            await socket.sendMessage(from, { text: '❌ දෝෂයක් සිදු විය: ' + e.message }, { quoted: msg });
        }
        break;
    } 
// ==========================================
// 🆕 STICKER MAKER (.sticker / .s)
// ==========================================
case 'sticker':
case 's': {
    const quotedMedia = msg.message.extendedTextMessage?.contextInfo?.quotedMessage;
    const directImage = msg.message.imageMessage;
    const targetImage = directImage || quotedMedia?.imageMessage;

    if (!targetImage) {
        return await socket.sendMessage(sender, {
            text: '❌ *Image ekakata reply karala* `.sticker` *danna, nathnam image ekaka caption ekata* `.sticker` *danna.*'
        }, { quoted: msg });
    }

    try {
        await socket.sendMessage(sender, { react: { text: '🖼️', key: msg.key } });
        const stream = await downloadContentFromMessage(targetImage, 'image');
        let buffer = Buffer.from([]);
        for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);

        const webpBuffer = await sharp(buffer)
            .resize(512, 512, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } })
            .webp()
            .toBuffer();

        await socket.sendMessage(sender, { sticker: webpBuffer }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
    } catch (err) {
        await socket.sendMessage(sender, { text: `❌ Sticker hadanna bari una: ${err.message}` }, { quoted: msg });
    }
    break;
}

// ==========================================
// 🆕 STICKER TO IMAGE (.toimg)
// ==========================================
case 'toimg': {
    const quotedMedia = msg.message.extendedTextMessage?.contextInfo?.quotedMessage;
    const stickerMsg = quotedMedia?.stickerMessage || msg.message.stickerMessage;

    if (!stickerMsg) {
        return await socket.sendMessage(sender, {
            text: '❌ *Sticker ekakata reply karala* `.toimg` *danna.*'
        }, { quoted: msg });
    }

    try {
        await socket.sendMessage(sender, { react: { text: '🔄', key: msg.key } });
        const stream = await downloadContentFromMessage(stickerMsg, 'sticker');
        let buffer = Buffer.from([]);
        for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);

        const pngBuffer = await sharp(buffer).png().toBuffer();
        await socket.sendMessage(sender, { image: pngBuffer, caption: '✅ *Converted!*' }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
    } catch (err) {
        await socket.sendMessage(sender, { text: `❌ Error: ${err.message}` }, { quoted: msg });
    }
    break;
}

// ==========================================
// 🆕 QR CODE GENERATOR (.qr)
// ==========================================
case 'qr':
case 'qrcode': {
    if (!args.length) {
        return await socket.sendMessage(sender, {
            text: '❌ *Usage:* `.qr <text or url>`\n\n*Example:* `.qr https://wa.me/94712345678`'
        }, { quoted: msg });
    }
    const qrText = args.join(' ');
    const qrUrl = `https://api.qrserver.com/v1/create-qr-code/?size=500x500&data=${encodeURIComponent(qrText)}`;
    try {
        await socket.sendMessage(sender, {
            image: { url: qrUrl },
            caption: `✅ *QR Code Generated*\n\n📝 *Content:* ${qrText}`
        }, { quoted: msg });
    } catch (err) {
        await socket.sendMessage(sender, { text: `❌ QR eka hadanna bari una: ${err.message}` }, { quoted: msg });
    }
    break;
}

// ==========================================
// 🆕 URL SHORTENER (.short)
// ==========================================
case 'short':
case 'shorten': {
    if (!args.length || !args[0].startsWith('http')) {
        return await socket.sendMessage(sender, {
            text: '❌ *Usage:* `.short <url>`\n\n*Example:* `.short https://example.com/long-link`'
        }, { quoted: msg });
    }
    try {
        const res = await axios.get(`https://tinyurl.com/api-create.php?url=${encodeURIComponent(args[0])}`, { timeout: 15000 });
        await socket.sendMessage(sender, {
            text: `🔗 *Short URL:*\n${res.data}`
        }, { quoted: msg });
    } catch (err) {
        await socket.sendMessage(sender, { text: `❌ URL eka short karanna bari una: ${err.message}` }, { quoted: msg });
    }
    break;
}

// ==========================================
// 🆕 WEATHER (.weather)
// ==========================================
case 'weather': {
    if (!args.length) {
        return await socket.sendMessage(sender, {
            text: '❌ *Usage:* `.weather Colombo`'
        }, { quoted: msg });
    }
    const city = args.join(' ');
    try {
        const res = await axios.get(
            `https://wttr.in/${encodeURIComponent(city)}?format=%l:+%C+%t+(feels+%f)%0A💧+Humidity:+%h%0A💨+Wind:+%w`,
            { timeout: 15000 }
        );
        await socket.sendMessage(sender, { text: `🌤️ *Weather Report*\n\n${res.data}` }, { quoted: msg });
    } catch (err) {
        await socket.sendMessage(sender, { text: `❌ Weather ganna bari una: ${err.message}` }, { quoted: msg });
    }
    break;
}

// ==========================================
// 🆕 CALCULATOR (.calc)
// ==========================================
case 'calc':
case 'calculate': {
    if (!args.length) {
        return await socket.sendMessage(sender, {
            text: '❌ *Usage:* `.calc 5*(3+2)`'
        }, { quoted: msg });
    }
    const expr = args.join(' ');
    if (!/^[0-9+\-*/().\s^%]+$/.test(expr)) {
        return await socket.sendMessage(sender, {
            text: '❌ *Wrong characters!* Numbers saha + - * / ( ) . witharak use karanna.'
        }, { quoted: msg });
    }
    try {
        const safeExpr = expr.replace(/\^/g, '**');
        const result = Function(`"use strict"; return (${safeExpr})`)();
        await socket.sendMessage(sender, { text: `🧮 *${expr}* = *${result}*` }, { quoted: msg });
    } catch (err) {
        await socket.sendMessage(sender, { text: '❌ *Invalid expression!*' }, { quoted: msg });
    }
    break;
}

// ==========================================
// 🆕 TAG ALL GROUP MEMBERS (.tagall)
// ==========================================
case 'tagall': {
    if (!isGroup) {
        return await socket.sendMessage(sender, { text: '❌ *Group ekaka witharak use karanna puluwan!*' }, { quoted: msg });
    }
    if (!isOwner && !isAdmins) {
        return await socket.sendMessage(sender, { text: '❌ *Group admin/owner witharai use karanna puluwan!*' }, { quoted: msg });
    }
    const tagText = args.length ? args.join(' ') : '📢 *Everyone!*';
    const mentions = participants.map(p => p.id);
    let listText = `${tagText}\n\n`;
    mentions.forEach(m => { listText += `➥ @${m.split('@')[0]}\n`; });
    await socket.sendMessage(sender, { text: listText, mentions }, { quoted: msg });
    break;
}

// ==========================================
// 🆕 GROUP INFO (.groupinfo)
// ==========================================
case 'groupinfo': {
    if (!isGroup) {
        return await socket.sendMessage(sender, { text: '❌ *Group ekaka witharak use karanna puluwan!*' }, { quoted: msg });
    }
    try {
        let info = `👥 *Group Info*\n\n`;
        info += `📛 *Name:* ${groupMetadata.subject}\n`;
        info += `🆔 *ID:* ${groupMetadata.id}\n`;
        info += `👤 *Members:* ${participants.length}\n`;
        info += `👑 *Admins:* ${groupAdmins.length}\n`;
        if (groupMetadata.desc) info += `📝 *Description:* ${groupMetadata.desc.substring(0, 200)}\n`;
        await socket.sendMessage(sender, { text: info }, { quoted: msg });
    } catch (err) {
        await socket.sendMessage(sender, { text: `❌ Error: ${err.message}` }, { quoted: msg });
    }
    break;
}

// ==========================================
// 🆕 OWNER CONTACT (.owner)
// ==========================================
case 'owner': {
    const ownerNum = (config.OWNER_NUMBERS && config.OWNER_NUMBERS[0]) || sanitizedNumber;
    await socket.sendMessage(sender, {
        text: `👑 *Bot Owner*\n\n📱 wa.me/${ownerNum}\n\n_Contact karanna bot related help walata._`
    }, { quoted: msg });
    break;
}

// ==========================================
// 🆕 PAIR COMMAND (.pair) - chat eken alut number ekak pair karanna
// ==========================================
case 'pair': {
    if (!isOwner) {
        return await socket.sendMessage(sender, {
            text: "❌ *Only the bot owner can use this command.*"
        }, { quoted: msg });
    }
    if (!args.length) {
        return await socket.sendMessage(sender, {
            text: `❌ *Usage:* \`.pair <number>\`\n\n*Example:*\n\`.pair 94712345678\`\n\n_Number eka country code ekath ekka danna (94, 91, 1, wage)._`
        }, { quoted: msg });
    }

    const targetNumber = args[0].replace(/[^0-9]/g, '');
    if (!targetNumber || targetNumber.length < 9) {
        return await socket.sendMessage(sender, {
            text: `❌ *Invalid number!* Country code ekath ekka full number eka danna.\nExample: \`.pair 94712345678\``
        }, { quoted: msg });
    }

    await socket.sendMessage(sender, {
        text: `⏳ *Generating pairing code for* \`${targetNumber}\`*...*`
    }, { quoted: msg });

    try {
        let responded = false;
        const mockRes = {
            headersSent: false,
            send: async (data) => {
                if (responded) return;
                responded = true;
                mockRes.headersSent = true;
                if (data?.code) {
                    await socket.sendMessage(sender, {
                        text: `🔑 *Pairing Code for* \`${targetNumber}\`*:*\n\n\`${data.code}\`\n\n📱 *WhatsApp > Linked Devices > Link with phone number* walin methana code eka danna.\n\n⏱️ _Code eka මිනිත්තු කිහිපයකින් expire වෙනවා - ඉක්මනට use කරන්න._`
                    }, { quoted: msg });
                } else if (data?.error) {
                    await socket.sendMessage(sender, { text: `❌ ${data.error}` }, { quoted: msg });
                }
            },
            status: (code) => {
                return {
                    send: async (data) => {
                        if (responded) return;
                        responded = true;
                        mockRes.headersSent = true;
                        await socket.sendMessage(sender, {
                            text: `❌ *Pairing failed* (status ${code}): ${data?.error || 'Unknown error'}`
                        }, { quoted: msg });
                    }
                };
            }
        };

        await EmpirePair(targetNumber, mockRes);
    } catch (e) {
        console.error('.pair command error:', e.message);
        await socket.sendMessage(sender, { text: `❌ Pairing eka fail una: ${e.message}` }, { quoted: msg });
    }
    break;
}
// ==========================================
// 🆕 SUBSCRIPTION MANAGEMENT (.add) - master owner witharai use karanna one
// ==========================================
case 'add': {
    // 🔒 94784224161 (master) witharai. Wena paired bot owners (isOwner) ta puluwan na.
    if (!(config.OWNER_NUMBERS || []).includes(senderNumber)) {
        return await socket.sendMessage(sender, {
            text: "❌ *Master owner witharai use karanna puluwan!*"
        }, { quoted: msg });
    }
    if (!args.length || !text.includes('|')) {
        return await socket.sendMessage(sender, {
            text: `❌ *Usage:* \`.add <number> | <months>\`\n\n*Example:*\n\`.add 94712345678 | 1\`\n\n_Masa 1ka subscription ekak denna._`
        }, { quoted: msg });
    }

    const rawInput = text.slice((sessionConfig.PREFIX || '!').length + command.length).trim();
    const parts = rawInput.split('|');
    if (parts.length < 2) {
        return await socket.sendMessage(sender, {
            text: `❌ *Usage:* \`.add <number> | <months>\``
        }, { quoted: msg });
    }

    const targetNumber = parts[0].trim().replace(/[^0-9]/g, '');
    const months = parseInt(parts[1].trim());

    if (!targetNumber || targetNumber.length < 9) {
        return await socket.sendMessage(sender, { text: `❌ *Invalid number!*` }, { quoted: msg });
    }
    if (isNaN(months) || months <= 0) {
        return await socket.sendMessage(sender, { text: `❌ *Invalid months!* Number ekak danna (1, 2, 3...).` }, { quoted: msg });
    }

    try {
        const targetSession = await Session.findOne({ number: targetNumber }, 'number');
        if (!targetSession) {
            return await socket.sendMessage(sender, {
                text: `❌ *${targetNumber}* mulinma bot eka pair karanna one!\n\n\`.pair ${targetNumber}\` use karanna, nathnam pairing web link eka eyata denna.`
            }, { quoted: msg });
        }

        const targetConfig = await loadUserConfig(targetNumber);
        const now = Date.now();
        const currentExpiry = targetConfig.LICENSE_EXPIRY ? new Date(targetConfig.LICENSE_EXPIRY).getTime() : 0;
        const baseTime = currentExpiry > now ? currentExpiry : now; // dan active nam eken passe extend karanawa
        const newExpiry = new Date(baseTime);
        newExpiry.setMonth(newExpiry.getMonth() + months);

        const updatedConfig = { ...targetConfig, LICENSE_EXPIRY: newExpiry.toISOString() };
        await updateUserConfig(targetNumber, updatedConfig);

        // Meema process eke thamayi eyage bot eka active nam, live widihatama update karanawa
        const liveEntry = activeSockets.get(targetNumber);
        if (liveEntry) {
            liveEntry.config = updatedConfig;
            activeSockets.set(targetNumber, liveEntry);
        }

        await socket.sendMessage(sender, {
            text: `✅ *Subscription Added!*\n\n📱 *Number:* ${targetNumber}\n📅 *Months:* ${months}\n⏳ *Expires:* ${newExpiry.toDateString()}`
        }, { quoted: msg });

        // Customer ta notify karanawa
        try {
            await socket.sendMessage(`${targetNumber}@s.whatsapp.net`, {
                text: `🎉 *Subscription Activated!*\n\nOyage bot eka dan *active*! ✅\n\n⏳ *Expires:* ${newExpiry.toDateString()}\n\n_Bot eka dan free widihata use karanna puluwan._`
            });
        } catch (notifyErr) {
            // number eka bot ekakma nam notify wenne nathi wenna puluwan, ok tibenna denna
        }

    } catch (err) {
        console.error('.add command error:', err.message);
        await socket.sendMessage(sender, { text: `❌ Error: ${err.message}` }, { quoted: msg });
    }
    break;
}

// ==========================================
// 🆕 SUBSCRIPTION STATUS (.license / .mysub)
// ==========================================
case 'license':
case 'mysub':
case 'subscription': {
    const isMasterBotNumber = (config.OWNER_NUMBERS || []).includes(sanitizedNumber);
    if (isMasterBotNumber) {
        return await socket.sendMessage(sender, {
            text: `👑 *Master Bot* - Subscription ekak one nah, permanent access.`
        }, { quoted: msg });
    }

    const expiryStr = sessionConfig.LICENSE_EXPIRY;
    if (!expiryStr) {
        return await socket.sendMessage(sender, {
            text: `⛔ *Payment Required!*\n\nActive subscription ekak nathi. *Pay karala ganna* owner ta contact karanna.\n👑 wa.me/${(config.OWNER_NUMBERS || [])[0] || ''}`
        }, { quoted: msg });
    }

    const expiry = new Date(expiryStr);
    const active = expiry.getTime() > Date.now();
    const daysLeft = Math.ceil((expiry.getTime() - Date.now()) / (1000 * 60 * 60 * 24));

    await socket.sendMessage(sender, {
        text: `${active ? '✅' : '⛔'} *Subscription Status*\n\n📅 *Expires:* ${expiry.toDateString()}\n${active ? `🟢 *Active* (${daysLeft} days left)` : '🔴 *Expired*'}\n\n${!active ? `Renew karanna owner ta contact karanna: wa.me/${(config.OWNER_NUMBERS || [])[0] || ''}` : ''}`
    }, { quoted: msg });
    break;
}
// ==========================================
// 🆕 ACCESS KEY COMMAND (.getkey) - web panel eke settings edit karanna one key eka
// ==========================================
case 'getkey':
case 'accesskey': {
    if (!isOwner) {
        return await socket.sendMessage(sender, {
            text: "❌ *Only the bot owner can use this command.*"
        }, { quoted: msg });
    }
    try {
        const accessKey = await ensureAccessKey(sanitizedNumber);
        await socket.sendMessage(sender, {
            text: `🔑 *Your Access Key:*\n\n\`${accessKey || 'N/A'}\`\n\n_Bot eke Name/Image/Footer/Movie Footer/Online-Offline/Auto-Like/Auto-Seen/Anti-Delete wage settings web panel eken venas karanna me key eka use karanna. Kawruth ekka share karanna epa._`
        }, { quoted: msg });
    } catch (e) {
        console.error('getkey error:', e.message);
        await socket.sendMessage(sender, { text: '❌ Access key eka ganna bari una.' }, { quoted: msg });
    }
    break;
}
// ==========================================
// SYSTEM CONFIGURATION & MONGODB SETTING COMMAND (.set)
// ==========================================
case 'button':
case 'buttons': {
    if (!isOwner) {
        return await socket.sendMessage(sender, { text: '❌ *Only the bot owner can use this command.*' }, { quoted: msg });
    }
    if (!buttonAllowed()) {
        return await socket.sendMessage(sender, { text: buttonLockedMsg }, { quoted: msg });
    }
    const opt = (args[0] || '').toLowerCase();
    const P2 = sessionConfig.PREFIX || config.PREFIX || '.';
    if (!['on', 'off'].includes(opt)) {
        return await socket.sendMessage(sender, {
            text: `🔘 *BUTTON MODE :* ${sessionConfig.BUTTON_MODE === 'true' ? 'ON ✅' : 'OFF ❌'}\n\n_${P2}button on_  — buttons ON\n_${P2}button off_ — number reply mode`
        }, { quoted: msg });
    }
    sessionConfig = { ...sessionConfig, BUTTON_MODE: opt === 'on' ? 'true' : 'false' };
    await updateUserConfig(sanitizedNumber, sessionConfig);
    activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });
    await socket.sendMessage(sender, {
        text: `🔘 *Button Mode ${opt === 'on' ? 'ON ✅' : 'OFF ❌'}*\n\n_${opt === 'on' ? 'Dan list / number replies buttons widihata enawa.' : 'Dan wenas kalaa numbers reply karala use karanna.'}_`
    }, { quoted: msg });
    break;
}

case 'set':
case 'setting': {
    if (!isOwner) {
        return await socket.sendMessage(sender, {
            text: "❌ *Only the bot owner can use this command.*"
        }, { quoted: msg });
    }

    const validKeys = [
        'PREFIX', 'AUTO_RECORDING', 'AUTO_TYPING', 'MODE', 'JID',
        'ALWAYS_ONLINE', 'ALWAYS_MSG_SEEN', 'STATUS_VIEW', 'AUTO_LIKE',
        'ANTI_DELETE', 'BUTTON_MODE', 'MOVIE_FOOTER', 'MOVIE_CAPTION', 'BOT_NAME', 'BOT_IMAGE', 'BOT_FOOTER', 'AIR_FOOTER'
    ];
    const BOOL_KEYS = ['AUTO_RECORDING', 'AUTO_TYPING', 'ALWAYS_ONLINE', 'ALWAYS_MSG_SEEN', 'STATUS_VIEW', 'AUTO_LIKE', 'ANTI_DELETE', 'BUTTON_MODE'];

    if (!args.length) {
        const P3 = sessionConfig.PREFIX || config.PREFIX || '.';
        const v = (k) => sessionConfig[k] ?? config[k] ?? '';
        const sw = (k) => (String(v(k)) === 'true' ? '✅' : '❌');
        const short = (t) => { t = String(t || '-').replace(/\s+/g, ' '); return t.length > 34 ? t.slice(0, 33) + '…' : t; };
        const dash =
`╭━━━〔 ⚙️ *𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦* ⚙️ 〕━━━╮
┃
┃ 🤖 *Name*     : ${short(v('BOT_NAME'))}
┃ 🔣 *Prefix*   : ${v('PREFIX') || '.'}
┃ 🌐 *Mode*     : ${v('MODE') || 'public'}
┃
┃ ${sw('ALWAYS_ONLINE')} ALWAYS_ONLINE
┃ ${sw('ALWAYS_MSG_SEEN')} ALWAYS_MSG_SEEN
┃ ${sw('AUTO_TYPING')} AUTO_TYPING
┃ ${sw('AUTO_RECORDING')} AUTO_RECORDING
┃ ${sw('STATUS_VIEW')} STATUS_VIEW
┃ ${sw('AUTO_LIKE')} AUTO_LIKE
┃ ${sw('ANTI_DELETE')} ANTI_DELETE
┃ ${buttonAllowed() ? sw('BUTTON_MODE') : '🔒'} BUTTON_MODE
┃
╰━━━━━━━━━━━━━━━━━━━━╯

📝 *Use :* \`${P3}set KEY:VALUE\`
✨ \`${P3}set ANTI_DELETE:on\`
🫧 \`${P3}set PREFIX:!, MODE:private\`

🔤 *Text keys:* BOT_NAME, BOT_FOOTER, MOVIE_FOOTER, MOVIE_CAPTION, BOT_IMAGE (link)
🌐 *MODE:* public / private / inbox / groups
⚡ _Wenas kirim ikmanata apply wenawa._`;
        return await socket.sendMessage(sender, {
            image: { url: pickBotImage(sessionConfig) },
            caption: dash + `\n\n> 🎭 *SHAGGY XMD* 🎭`
        }, { quoted: msg });
    }

    const input = args.join(' ');
    const updates = {};
    // comma eken wen karanne "KEY:" ekak pahalin enawa nam witharai (text values wala comma thiyenna puluwan)
    const pairs = input.split(/,(?=\s*[A-Za-z_]+\s*:)/);
    let hasInvalidKey = false;
    let invalidKeyName = '';
    let problem = '';

    for (const pair of pairs) {
        const idx = pair.indexOf(':');
        if (idx === -1) continue;
        const key = pair.slice(0, idx).trim().toUpperCase();
        let value = pair.slice(idx + 1).trim();
        if (!validKeys.includes(key)) { hasInvalidKey = true; invalidKeyName = key; continue; }

        if (BOOL_KEYS.includes(key)) {
            const lv = value.toLowerCase();
            if (['true', 'on', 'yes', '1'].includes(lv)) value = 'true';
            else if (['false', 'off', 'no', '0'].includes(lv)) value = 'false';
            else { problem = `\`${key}\` ekata *on / off* (ho true / false) denna.`; break; }
            if (key === 'BUTTON_MODE' && value === 'true' && !buttonAllowed()) { problem = buttonLockedMsg; break; }
        } else if (key === 'MODE') {
            value = value.toLowerCase();
            if (!['public', 'private', 'inbox', 'groups'].includes(value)) { problem = '`MODE` = public / private / inbox / groups'; break; }
        } else if (key === 'PREFIX') {
            if (!value || value.length > 3) { problem = '`PREFIX` eka characters 1-3ka wenna one.'; break; }
        } else if (key === 'BOT_IMAGE') {
            if (value && !/^https?:\/\//i.test(value)) { problem = '`BOT_IMAGE` eka http/https link ekak wenna one.'; break; }
        }
        updates[key] = value;
    }
    if (problem) {
        return await socket.sendMessage(sender, { text: `❌ ${problem}` }, { quoted: msg });
    }

    if (hasInvalidKey) {
        return await socket.sendMessage(sender, {
            text: `Invalid system key: \`${invalidKeyName}\`\n\n> ${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`
        }, { quoted: msg });
    }

    if (Object.keys(updates).length === 0) {
        return await socket.sendMessage(sender, { text: "🎀 *𝗙𝗢𝗥𝗠𝗔𝗧  𝗘𝗥𝗥𝗢𝗥:* Please use `Key:Value` structure." });
    }

    try {
        await socket.sendMessage(sender, { react: { text: "⚙️", key: msg.key } });

        // 1. Session සහ Database එක රියල්-ටයිම් අප්ඩේට් කිරීම
        sessionConfig = { ...sessionConfig, ...updates };

        // MongoDB වෙත ඩේටා නිවැරදිව සේව් වීම සඳහා updateUserConfig හෝ Mongoose Model එක හරහා ස්ථිරවම Save කරයි
        await updateUserConfig(sanitizedNumber, sessionConfig);

        // Active Sockets වලට අලුත් කොන්ෆිග් එක රියල්-ටයිම් ලෝඩ් කිරීම
        activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });

        let updateSummary = Object.entries(updates).map(([k, v]) => {
            let displayVal = Array.isArray(v) ? v.join(' ') : v;
            return `🎀 *${k}* ──❯ \`${displayVal}\``;
        }).join('\n');

        const successMsg = `🎀 *𝗖𝗢𝗡𝗙𝗜𝗚𝗨𝗥𝗔𝗧𝗜𝗢𝗡  𝗨𝗣𝗗𝗔𝗧𝗘𝗗*\n\n` +
            `${updateSummary}\n\n` +
            `🫧 _System cloud & MongoDB changes applied successfully._`;

        await socket.sendMessage(sender, {
            image: { url: config.BOT_IMAGE || config.ERROR },
            caption: formatMessage(
                `✅ 𝗨𝗣𝗗𝗔𝗧𝗘  𝗦𝗨𝗖𝗖𝗘𝗦𝗦  ✅`,
                successMsg,
                `${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`
            )
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: "✨", key: msg.key } });

    } catch (error) {
        console.error("Update Error:", error);
        await socket.sendMessage(sender, { text: "🎀 " + error.message });
    }
}
break;
        }
        } catch (error) {
            console.error('Command handler error:', error);
            await socket.sendMessage(sender, {
                text: `❌ ERROR\nAn error occurred: ${error.message}`,
            });
        }
    });
}

async function setupMessageHandlers(socket) {
    const messageHandler = async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

        const senderNumber = msg.key.participant ? msg.key.participant.split('@')[0] : msg.key.remoteJid.split('@')[0];
        const botNumber = jidNormalizedUser(socket.user.id).split('@')[0];
        const isReact = msg.message.reactionMessage;

        const sanitizedNumber = botNumber.replace(/[^0-9]/g, '');
        const sessionConfig = activeSockets.get(sanitizedNumber)?.config || config;

        if (sessionConfig.AUTO_TYPING === 'true') {
            try {
                await socket.sendPresenceUpdate('composing', msg.key.remoteJid);
            } catch (error) {

            }
        }

        if (sessionConfig.AUTO_RECORDING === 'true') {
            try {
                await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
            } catch (error) {

            }
        }

        if (!isReact && senderNumber !== botNumber) {
            if (sessionConfig.AUTO_REACT === 'true') {
                const reactions = [
                    '❤', '💕', '😻', '🧡', '💛', '💚', '💙', '💜', '🖤', '❣', '💞', '💓', '💗',
                    '💖', '💘', '💝', '💟', '♥', '💌', '🙂', '🤗', '😌', '😉', '🤗', '😊',
                    '🎊', '🎉', '🎁', '🎈', '👋'
                ];
                const randomReaction = reactions[Math.floor(Math.random() * reactions.length)];

                await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 1000));

                try {
                    await socket.sendMessage(msg.key.remoteJid, { react: { text: randomReaction, key: msg.key } });
                } catch (error) {

                }
            }
        }
    };

    socket.ev.on('messages.upsert', messageHandler);
    return () => {
        socket.ev.off('messages.upsert', messageHandler);

    };
}

async function saveSession(number, creds) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            { creds, updatedAt: new Date() },
            { upsert: true }
        );
        const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        fs.ensureDirSync(sessionPath);
        fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(creds, null, 2));
        let numbers = [];
        if (fs.existsSync(NUMBER_LIST_PATH)) {
            numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
        }
        if (!numbers.includes(sanitizedNumber)) {
            numbers.push(sanitizedNumber);
            fs.writeFileSync(NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
        }
    } catch (error) {

    }
}

async function restoreSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const session = await Session.findOne({ number: sanitizedNumber });
        if (!session || !session.creds || !session.creds.me || !session.creds.me.id) {
            await deleteSession(sanitizedNumber);
            return null;
        }
        const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        fs.ensureDirSync(sessionPath);
        fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(session.creds, null, 2));
        return session.creds;
    } catch (error) {
        return null;
    }
}

async function deleteSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        await Session.deleteOne({ number: sanitizedNumber });
        const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        if (fs.existsSync(sessionPath)) {
            fs.removeSync(sessionPath);
        }
        if (fs.existsSync(NUMBER_LIST_PATH)) {
            let numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
            numbers = numbers.filter(n => n !== sanitizedNumber);
            fs.writeFileSync(NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
        }
    } catch (error) {

    }
}

async function loadUserConfig(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const configDoc = await Session.findOne({ number: sanitizedNumber }, 'config');
        return { ...config, ...configDoc?.config };
    } catch (error) {
        console.error(`Failed to load config for ${number}:`, error);
        return { ...config };
    }
}

async function updateUserConfig(number, newConfig) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    try {
        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            { config: newConfig, updatedAt: new Date() },
            { upsert: true }
        );
        console.log(`Updated config for ${sanitizedNumber}`);
    } catch (error) {
        console.error(`Failed to update config for ${sanitizedNumber}:`, error);
        throw error;
    }
}

// 🆕 Random access key ekak generate karanawa (web panel eken bot settings edit karanna)
function generateAccessKey() {
    return crypto.randomBytes(5).toString('hex').toUpperCase(); // e.g. "A1B2C3D4E5"
}

// 🆕 Number ekakata accessKey ekak thiyenawada balala, nathnam alutin generate karala DB eke save karanawa.
async function ensureAccessKey(number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    try {
        const doc = await Session.findOne({ number: sanitizedNumber }, 'config');
        const currentConfig = doc?.config || {};
        if (currentConfig.accessKey) {
            return currentConfig.accessKey;
        }
        const accessKey = generateAccessKey();
        const mergedConfig = { ...currentConfig, accessKey };
        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            { config: mergedConfig, updatedAt: new Date() },
            { upsert: true }
        );
        return accessKey;
    } catch (error) {
        console.error(`Failed to ensure access key for ${sanitizedNumber}:`, error);
        return null;
    }
} 
function setupAutoRestart(socket, number) {
    const maxReconnectAttempts = 10;
    let reconnectAttempts = 0;

    socket.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'close' && lastDisconnect?.error?.output?.statusCode !== 401) {
            if (reconnectAttempts >= maxReconnectAttempts) {
                activeSockets.delete(number.replace(/[^0-9]/g, ''));
                socketCreationTime.delete(number.replace(/[^0-9]/g, ''));
                return;
            }
            console.log(`Connection lost for ${number}, attempt ${reconnectAttempts + 1}/${maxReconnectAttempts}`);
            try {
                await delay(5000 * (reconnectAttempts + 1));
                activeSockets.delete(number.replace(/[^0-9]/g, ''));
                socketCreationTime.delete(number.replace(/[^0-9]/g, ''));
                const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
                await EmpirePair(number, mockRes);
                reconnectAttempts = 0;
            } catch (error) {
                console.error(`Reconnect failed for ${number}:`, error);
                reconnectAttempts++;
            }
        } else if (connection === 'open') {
            reconnectAttempts = 0;
            console.log(`Connection established for ${number}`);
        }
    });
}
async function EmpirePair(number, res) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);

    await restoreSession(sanitizedNumber);
    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);

    try {
        const { version } = await fetchLatestBaileysVersion();
        const socket = makeWASocket({
            auth: state,
            printQRInTerminal: false,
            version,
            browser: Browsers.macOS('Safari'),
        });

        socketCreationTime.set(sanitizedNumber, Date.now());
        setupCommandHandlers(socket, sanitizedNumber);
        setupAutoReply(socket, sanitizedNumber);
        setupAutoRestart(socket, sanitizedNumber);
        if (!socket.authState.creds.registered) {
            let retries = config.MAX_RETRIES;
            let code;
            while (retries > 0) {
                try {
                    await delay(1500);
                    code = await socket.requestPairingCode(sanitizedNumber);
                    break;
                } catch (error) {
                    retries--;
                    if (retries === 0) throw error;
                    await delay(2000 * (config.MAX_RETRIES - retries));
                }
            }
            if (!res.headersSent) res.send({ code });
        }
        socket.ev.on('creds.update', async () => {
            try {
                await saveCreds();
                const credsPath = path.join(sessionPath, 'creds.json');
                if (!fs.existsSync(credsPath)) return;
                const creds = JSON.parse(await fs.readFile(credsPath, 'utf8'));
                await saveSession(sanitizedNumber, creds);
            } catch (error) {
            }
        });
        socket.ev.on('connection.update', async (update) => {
            const { connection } = update;

            if (connection === 'open') {
                try {
                    await delay(3000);

                    const userJid = jidNormalizedUser(socket.user.id);
                    let sessionConfig = await loadUserConfig(sanitizedNumber);
                    activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });

                    // 🆕 ALWAYS_ONLINE respect karanawa - force unavailable karanne nah dan
                    const initialPresence = sessionConfig.ALWAYS_ONLINE === 'true' ? 'available' : 'unavailable';
                    await socket.sendPresenceUpdate(initialPresence);
                    try {
                        const lidStore = socket.signalRepository.lidMapping;

                        if (isPnUser(userJid)) {
                            const lid = await lidStore.getLIDForPN(userJid);
                            console.log(`✅ ${sanitizedNumber} → PN: ${userJid} → LID: ${lid}`);
                        }
                    } catch (lidError) {
                        console.log(`⚠️ LID mapping not available yet for ${sanitizedNumber}:`, lidError.message);
                    }

                    // ⚠️ IMPORTANT: pena presence interval ekak thiyenawa nam mulinma clear karanawa
                    if (presenceIntervals.has(sanitizedNumber)) {
                        clearInterval(presenceIntervals.get(sanitizedNumber));
                    }
                    const presenceInterval = setInterval(() => {
                        // 🆕 Interval eka run wena hama welavakama .set eken update kalath dan latest config eka gannawa
                        const liveConfig = activeSockets.get(sanitizedNumber)?.config || sessionConfig;
                        const presence = liveConfig.ALWAYS_ONLINE === 'true' ? 'available' : 'unavailable';
                        socket.sendPresenceUpdate(presence).catch(() => {});
                    }, 30000);
                    presenceIntervals.set(sanitizedNumber, presenceInterval);

                    // 🆕 Access key ekak nathnam alutin hadanawa (thiyenawa nam eka ma denawa)
                    const accessKey = await ensureAccessKey(sanitizedNumber);

                    // Welcome Message
                    await socket.sendMessage(userJid, {
                        image: { url: pickBotImage(sessionConfig) },
                        caption: formatMessage(
                            '✨ *Bot Activated!*',
                            `📱 *Number:* ${sanitizedNumber}
🕒 *Time:* ${getSriLankaTimestamp()}
🟢 *Status:* Online
🔑 *Access Key:* ${accessKey || 'N/A'}

_Bot eke Name/Image/Footer/Movie Footer/Online-Offline/Auto-Like/Auto-Seen/Anti-Delete web panel eken venas karanna me Access Key eka use karanna. Kawruth ekka share karanna epa._`,
                            '🇸‌ʜᴀɢɢY 🇽‌ᴍᴅ'
                        )
                    });

                } catch (error) {
                    console.error(`Error in connection.open for ${sanitizedNumber}:`, error);
                    exec(`pm2 restart ${process.env.PM2_NAME || '{LAKIYA-{M𝙳-{F𝚁𝙴𝙴-{B𝙾𝚃-session'}`);
                }
            }
        });

    } catch (error) {
        console.error('Pairing/reconnect error:', error);
        socketCreationTime.delete(sanitizedNumber);
        if (!res.headersSent) res.status(503).send({ error: 'Service Unavailable' });
    }
}

// 🌐 Home page status (main.html)
router.get('/status', (req, res) => {
    res.set('Cache-Control', 'no-store');
    res.json({ online: true, bots: activeSockets.size, uptime: Math.floor(process.uptime()), version: 'v4' });
});

router.get('/', async (req, res) => {
    const { number } = req.query;
    if (!number) {
        return res.status(400).send({ error: 'Number parameter is required' });
    }

    const sanitizedNumber = number.replace(/[^0-9]/g, '');

    if (activeSockets.has(sanitizedNumber)) {
        try {
            const oldSocket = activeSockets.get(sanitizedNumber);
            if (oldSocket && oldSocket.socket) {
                try {
                    await oldSocket.socket.logout();
                    oldSocket.socket.end();
                    oldSocket.socket.ws?.close();
                } catch (e) {
                    console.log('Socket close error:', e.message);
                }
            }
            activeSockets.delete(sanitizedNumber);
            socketCreationTime.delete(sanitizedNumber);
            await Session.deleteOne({ number: sanitizedNumber });
            const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
            if (fs.existsSync(sessionPath)) {
                fs.removeSync(sessionPath);
            }
            if (fs.existsSync(NUMBER_LIST_PATH)) {
                let numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
                numbers = numbers.filter(n => n !== sanitizedNumber);
                fs.writeFileSync(NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
            }
            console.log(`✅ Old session removed for: ${sanitizedNumber} - Creating new pairing`);
        } catch (error) {
            console.error('Error removing old session:', error);
        }
    }

    await EmpirePair(number, res);
});

process.on('exit', () => {
    activeSockets.forEach((socket, number) => {
        socket.ws.close();
        activeSockets.delete(number);
        socketCreationTime.delete(number);
    });
    fs.emptyDirSync(SESSION_BASE_PATH);
});

process.on('uncaughtException', (err) => {
    console.error('Uncaught exception:', err);
    exec(`pm2 restart ${process.env.PM2_NAME || '{test-{md-{mini-{bot-session'}`);
});

export default router;
