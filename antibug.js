// 🛡️ BUG SHIELD (anti-crash / anti-bug)
// ------------------------------------------------------------------
// Bug / crash messages (empty list/button/interactive flood, huge text, invisible-char
// payloads, mention floods, nested quotes ...) bot ekata handler walata yanna issella
// *ugullanawa*, chat eken delete karanawa, flood karana ekata block karanawa.
//
// Wade karana widiha:
//   socket.ev.on('messages.upsert', fn) kiyana hama listener ekakma wrap karanawa.
//   Listener ekata yanna kalin message eka scan karanawa. Bug ekak nam payload eken
//   ayin karanawa (handlers walata yanne na => cache / JSON / crash nehe),
//   ita passe "delete for me" + (optional) block + owner ta notice.
//   Listener eke throw / unhandled rejection awath process eka crash wenne na.
//
// ⚠️ installBugShield() eka makeWASocket() ekata passe, setupCommandHandlers() walata
//    *kalin* call karanna oni (listeners attach wenna kalin).

const LIMITS = {
    TEXT_MAX: 30000,          // conversation / text / caption
    FIELD_MAX: 8000,          // wenath string field ekak (vcard, paramsJson, title ...)
    SMALL_FIELD_MAX: 3000,    // name / address / fileName / title wage
    MAX_NODES: 6000,
    MAX_DEPTH: 30,
    MAX_BYTES: 400_000,       // total binary (thumbnail wage) bytes
    MAX_ARRAY: 1500,
    MAX_MENTIONS: 300,
    MAX_POLL_OPTIONS: 20,
    MAX_CONTACTS: 10,
    FLOOD_COUNT: 25,          // me window eka athule
    FLOOD_WINDOW_MS: 10_000,
    QUARANTINE_MS: 10 * 60_000,
    DELETE_CHAT_AT: 6,        // bug messages meka wada unoth chat eka tikkama delete
    NOTIFY_EVERY_MS: 10 * 60_000
};

const TEXT_KEYS = new Set(['conversation', 'text', 'caption', 'body', 'contentText', 'description', 'footerText', 'footer']);
const SMALL_KEYS = new Set([
    'displayName', 'name', 'address', 'fileName', 'title', 'buttonText', 'selectedDisplayText',
    'hydratedTitleText', 'hydratedContentText', 'hydratedFooterText', 'paramsJson', 'buttonParamsJson',
    'selectedId', 'selectedRowId', 'selectedButtonId', 'pushName', 'matchedText', 'canonicalUrl'
]);

// Incoming wela *apita ona nathi* (bug walata godak use wena) message types.
// (buttonsResponse / listResponse / templateButtonReply / interactiveResponse allowed - bot ekata ewa ona)
const INTERACTIVE_TYPES = new Set([
    'listMessage', 'buttonsMessage', 'interactiveMessage', 'templateMessage',
    'highlyStructuredMessage', 'productMessage', 'orderMessage', 'requestPaymentMessage',
    'paymentInviteMessage', 'sendPaymentMessage', 'scheduledCallCreationMessage',
    'scheduledCallEditMessage', 'cancelPaymentRequestMessage', 'declinePaymentRequestMessage',
    'interactiveMessage', 'listMessageV2', 'carouselMessage'
]);
const SKIP_KEYS = new Set(['messageContextInfo', 'senderKeyDistributionMessage', 'messageSecret']);

// invisible / zalgo chars (Sinhala walata ona ZWJ \u200D & ZWNJ \u200C ayin karala thiyenne)
const INVIS_RE = /[\u200B\u200E\u200F\u202A-\u202E\u2060-\u2064\u206A-\u206F\uFEFF\u180E\u115F\u1160\u3164]/g;
const ZALGO_RE = /[\u0300-\u036F\u0483-\u0489\u0591-\u05BD\u0610-\u061A\u064B-\u065F]/g;
const CRASHSCRIPT_RE = /[\uA980-\uA9DF\u0F00-\u0FFF\u1100-\u11FF\uD800-\uDBFF]{60,}/;

const digits = (j) => String(j || '').split('@')[0].split(':')[0].replace(/[^0-9]/g, '');
const isGroup = (j) => String(j || '').endsWith('@g.us');
const toSec = (t) => {
    try { const n = Number(t?.toNumber ? t.toNumber() : t); return n > 0 ? n : Math.floor(Date.now() / 1000); }
    catch { return Math.floor(Date.now() / 1000); }
};

function unwrap(message) {
    let m = message || {};
    for (let i = 0; i < 8; i++) {
        const inner = m.ephemeralMessage?.message
            || m.viewOnceMessage?.message
            || m.viewOnceMessageV2?.message
            || m.viewOnceMessageV2Extension?.message
            || m.documentWithCaptionMessage?.message
            || m.editedMessage?.message
            || m.botInvokeMessage?.message;
        if (!inner) break;
        m = inner;
    }
    return m;
}

function weirdText(s) {
    if (s.length < 300) return null;
    const invis = (s.match(INVIS_RE) || []).length;
    if (invis > 3000 || (invis > 400 && invis / s.length > 0.3)) return 'invisible-char payload';
    const zalgo = (s.match(ZALGO_RE) || []).length;
    if (zalgo > 2500 || (zalgo > 600 && zalgo / s.length > 0.3)) return 'zalgo / combining-char payload';
    if (CRASHSCRIPT_RE.test(s)) return 'crash-script payload';
    return null;
}

/** Message eka scan karala bug ekak nam reason ekak denawa, nathnam null. */
function scanPayload(root) {
    let nodes = 0, bytes = 0;
    const stack = [[root, 0, '']];
    while (stack.length) {
        const [o, d, k] = stack.pop();
        if (o == null) continue;
        const t = typeof o;
        if (t === 'string') {
            const max = TEXT_KEYS.has(k) ? LIMITS.TEXT_MAX : SMALL_KEYS.has(k) ? LIMITS.SMALL_FIELD_MAX : LIMITS.FIELD_MAX;
            if (o.length > max) return `oversized field (${k || 'text'}: ${o.length})`;
            const w = weirdText(o);
            if (w) return w;
            continue;
        }
        if (t !== 'object') continue;
        if (o instanceof Uint8Array) {
            bytes += o.byteLength;
            if (bytes > LIMITS.MAX_BYTES) return 'oversized binary payload';
            continue;
        }
        if (++nodes > LIMITS.MAX_NODES) return 'too many nodes';
        if (d > LIMITS.MAX_DEPTH) return 'nesting too deep';
        if (Array.isArray(o)) {
            if (o.length > LIMITS.MAX_ARRAY) return 'huge array';
            if (k === 'mentionedJid' && o.length > LIMITS.MAX_MENTIONS) return `mention flood (${o.length})`;
            for (const v of o) stack.push([v, d + 1, k]);
            continue;
        }
        for (const key of Object.keys(o)) stack.push([o[key], d + 1, key]);
    }
    return null;
}

/** Type-specific checks (cheap). */
function checkTypes(m, blockInteractive) {
    const types = Object.keys(m).filter((x) => !SKIP_KEYS.has(x));
    if (blockInteractive) {
        const hit = types.find((x) => INTERACTIVE_TYPES.has(x));
        if (hit) return `incoming ${hit}`;
    }
    const poll = m.pollCreationMessage || m.pollCreationMessageV2 || m.pollCreationMessageV3;
    if (poll && (poll.options?.length || 0) > LIMITS.MAX_POLL_OPTIONS) return 'poll option flood';
    if (m.contactsArrayMessage && (m.contactsArrayMessage.contacts?.length || 0) > LIMITS.MAX_CONTACTS) return 'contact flood';
    const loc = m.locationMessage || m.liveLocationMessage;
    if (loc) {
        const { degreesLatitude: la, degreesLongitude: lo } = loc;
        if ((la != null && Math.abs(la) > 90) || (lo != null && Math.abs(lo) > 180)) return 'invalid location';
    }
    for (const key of ['imageMessage', 'videoMessage', 'stickerMessage']) {
        const x = m[key];
        if (x && (Number(x.width) > 16000 || Number(x.height) > 16000)) return `absurd ${key} dimensions`;
    }
    return null;
}

/**
 * @param socket   Baileys socket
 * @param opts.number        bot number (digits)
 * @param opts.ownerNumbers  owner numbers (digits[])
 * @param opts.getOptions    () => ({ enabled, autoBlock, notify, blockInteractive })
 * @param opts.canRevoke     async (groupJid) => boolean   (bot group admin nam true)
 */
export function installBugShield(socket, opts = {}) {
    const tag = `[BugShield:${opts.number || '?'}]`;
    const getOpts = () => ({ enabled: true, autoBlock: true, notify: true, blockInteractive: true, ...(opts.getOptions?.() || {}) });
    const owners = new Set([...(opts.ownerNumbers || []), opts.number].filter(Boolean).map(digits));

    const stats = { scanned: 0, blocked: 0, deleted: 0, quarantined: 0, errors: 0 };
    const recent = new Map();       // sender -> [timestamps]
    const quarantine = new Map();   // sender -> until
    const strikes = new Map();      // sender -> bug count
    const blockedJids = new Set();
    const notified = new Map();     // sender -> last notify
    const queue = new Map();        // chat -> { items: [{key, ts}], timer, reason, sender, info }

    const gc = setInterval(() => {
        const now = Date.now();
        for (const [k, v] of quarantine) if (v < now) { quarantine.delete(k); strikes.delete(k); }
        for (const [k, arr] of recent) if (!arr.length || now - arr[arr.length - 1] > 60_000) recent.delete(k);
        for (const [k, v] of notified) if (now - v > 3_600_000) notified.delete(k);
    }, 60_000);
    gc.unref?.();

    const isOwnerMsg = (key) => {
        const cands = [key.remoteJid, key.remoteJidAlt, key.participant, key.participantAlt];
        return cands.some((j) => j && owners.has(digits(j)));
    };

    // ---------- actions ----------
    async function deleteForMe(chat, item) {
        const { key, ts } = item;
        // Baileys versions walata anuwa API wenas - kramayen try karanawa
        try { await socket.chatModify({ deleteForMe: { deleteMedia: false, key, timestamp: ts } }, chat); return true; } catch { /* next */ }
        try { await socket.chatModify({ clear: { messages: [{ id: key.id, fromMe: false, timestamp: ts }] } }, chat); return true; } catch { /* next */ }
        return false;
    }

    async function deleteWholeChat(chat, last) {
        try {
            await socket.chatModify({ delete: true, lastMessages: [{ key: last.key, messageTimestamp: last.ts }] }, chat);
            return true;
        } catch (e) { console.error(tag, 'delete chat failed:', e.message); return false; }
    }

    async function blockSender(entry) {
        const targets = [entry.pnJid, entry.chat].filter((j, i, a) => j && a.indexOf(j) === i);
        for (const j of targets) {
            if (blockedJids.has(j)) return;
            try { await socket.updateBlockStatus(j, 'block'); blockedJids.add(j); stats.blocked++; return; }
            catch (e) { console.error(tag, 'block failed', j, e.message); }
        }
    }

    async function notifyOwner(entry, extra = '') {
        const now = Date.now();
        if (now - (notified.get(entry.sender) || 0) < LIMITS.NOTIFY_EVERY_MS) return;
        notified.set(entry.sender, now);
        try {
            const me = String(socket.user?.id || '').split(':')[0].split('@')[0] + '@s.whatsapp.net';
            await socket.sendMessage(me, {
                text: `🛡️ *BUG SHIELD*\n\n🚫 Bug / crash message ekak block kala\n👤 wa.me/${entry.sender || 'unknown'}\n📌 ${entry.reason}\n🗑️ Deleted: ${entry.count}${extra}\n\n_Chat eke bug messages auto delete wuna._`
            });
        } catch { /* ignore */ }
    }

    async function flush(chat) {
        const entry = queue.get(chat);
        if (!entry) return;
        queue.delete(chat);
        const o = getOpts();
        try {
            const group = isGroup(chat);
            if (group) {
                let admin = false;
                try { admin = !!(await opts.canRevoke?.(chat)); } catch { /* ignore */ }
                for (const it of entry.items.slice(0, 30)) {
                    if (admin) { try { await socket.sendMessage(chat, { delete: it.key }); stats.deleted++; continue; } catch { /* fallthrough */ } }
                    if (await deleteForMe(chat, it)) stats.deleted++;
                    await new Promise((r) => setTimeout(r, 250));
                }
            } else if (entry.items.length >= LIMITS.DELETE_CHAT_AT) {
                const last = entry.items[entry.items.length - 1];
                if (await deleteWholeChat(chat, last)) stats.deleted += entry.items.length;
            } else {
                for (const it of entry.items) {
                    if (await deleteForMe(chat, it)) stats.deleted++;
                    await new Promise((r) => setTimeout(r, 250));
                }
            }
            if (!group && o.autoBlock && !entry.isOwner) await blockSender(entry);
            if (o.notify) await notifyOwner({ ...entry, count: entry.total }, !group && o.autoBlock && !entry.isOwner ? '\n🔒 Sender block kala' : '');
        } catch (e) {
            stats.errors++;
            console.error(tag, 'flush error:', e.message);
        }
    }

    function enqueue(msg, reason) {
        const key = msg.key;
        const chat = key.remoteJid;
        const sender = digits(isGroup(chat) ? (key.participantAlt || key.participant) : (key.remoteJidAlt || chat));
        const pnJid = [key.remoteJidAlt, key.participantAlt].find((j) => j && String(j).endsWith('@s.whatsapp.net'))
            || (String(chat).endsWith('@s.whatsapp.net') ? chat : null);
        let e = queue.get(chat);
        if (!e) {
            e = { items: [], total: 0, reason, sender, chat, pnJid, isOwner: isOwnerMsg(key), timer: null };
            queue.set(chat, e);
        }
        if (e.items.length < 200) e.items.push({ key: { remoteJid: key.remoteJid, id: key.id, fromMe: false, participant: key.participant }, ts: toSec(msg.messageTimestamp) });
        e.total++;
        clearTimeout(e.timer);
        e.timer = setTimeout(() => flush(chat).catch(() => {}), 1500);   // debounce: flood ekak ekapaharata handle karanawa
        e.timer.unref?.();
    }

    // ---------- classify ----------
    /** @returns reason string if the message must be dropped, else null */
    function classify(msg, o) {
        const key = msg?.key;
        if (!key || !msg.message) return null;          // stub / receipts - allow
        if (key.fromMe) return null;
        if (key.remoteJid === 'status@broadcast') {
            // status eke bug payload walata only size check
            return scanPayload(msg.message);
        }
        stats.scanned++;
        const sender = digits(isGroup(key.remoteJid) ? (key.participantAlt || key.participant) : (key.remoteJidAlt || key.remoteJid));
        const owner = isOwnerMsg(key);

        // quarantine: kalin flood/bug karapu ekek (owner nam nehe)
        if (!owner && quarantine.has(sender) && quarantine.get(sender) > Date.now()) return 'quarantined sender';

        // flood detector
        if (!owner) {
            const now = Date.now();
            const arr = recent.get(sender) || [];
            arr.push(now);
            while (arr.length && now - arr[0] > LIMITS.FLOOD_WINDOW_MS) arr.shift();
            recent.set(sender, arr);
            if (arr.length > LIMITS.FLOOD_COUNT) {
                quarantine.set(sender, now + LIMITS.QUARANTINE_MS);
                stats.quarantined++;
                return 'message flood';
            }
        }

        let reason = null;
        try {
            const m = unwrap(msg.message);
            reason = checkTypes(m, o.blockInteractive && !owner) || scanPayload(msg.message);
            if (!reason && msg.pushName && String(msg.pushName).length > 300) reason = 'oversized pushName';
        } catch (e) {
            reason = 'unparsable message';      // scan eka crash unoth eka bug ekak kiyala gannawa
        }
        if (reason && !owner) {
            const n = (strikes.get(sender) || 0) + 1;
            strikes.set(sender, n);
            if (n >= 3) quarantine.set(sender, Date.now() + LIMITS.QUARANTINE_MS);
        }
        return reason;
    }

    function filterPayload(payload) {
        const o = getOpts();
        if (!o.enabled || !payload || !Array.isArray(payload.messages)) return payload;
        const clean = [];
        for (const msg of payload.messages) {
            let reason = null;
            try { reason = classify(msg, o); } catch (e) { reason = 'shield error'; stats.errors++; }
            if (!reason) { clean.push(msg); continue; }
            // quarantine / flood walata delete kranne bug eka thiyenawanam witharai
            const drop = reason;
            try {
                if (msg?.key && !msg.key.fromMe && msg.key.remoteJid !== 'status@broadcast') enqueue(msg, drop);
            } catch (e) { stats.errors++; }
            console.log(tag, `dropped ${msg?.key?.id || ''} -> ${drop}`);
        }
        return { ...payload, messages: clean };
    }

    // ---------- hook socket.ev.on / off ----------
    const ev = socket.ev;
    const wrapped = new WeakMap();
    const filteredCache = new WeakMap();   // payload -> filtered payload (listeners godak unath ekapaara witharai scan karanne)
    const origOn = ev.on.bind(ev);
    const origOff = (ev.off || ev.removeListener).bind(ev);

    const guard = (fn) => {
        const w = (payload) => {
            let p;
            try {
                if (payload && typeof payload === 'object') {
                    p = filteredCache.get(payload);
                    if (!p) { p = filterPayload(payload); filteredCache.set(payload, p); }
                } else p = payload;
            } catch (e) { stats.errors++; return; }
            if (p?.messages && p.messages.length === 0) return;      // ellam bug nam handler ekata yawanne na
            try {
                const r = fn(p);
                if (r && typeof r.catch === 'function') r.catch((e) => { stats.errors++; console.error(tag, 'handler rejected:', e?.message); });
                return r;
            } catch (e) { stats.errors++; console.error(tag, 'handler threw:', e?.message); }
        };
        return w;
    };

    ev.on = (event, fn) => {
        if (event !== 'messages.upsert' || typeof fn !== 'function') return origOn(event, fn);
        let w = wrapped.get(fn);
        if (!w) { w = guard(fn); wrapped.set(fn, w); }
        return origOn(event, w);
    };
    const offImpl = (event, fn) => origOff(event, (event === 'messages.upsert' && wrapped.get(fn)) || fn);
    ev.off = offImpl;
    ev.removeListener = offImpl;

    socket.bugShield = { stats, quarantine, scan: scanPayload, limits: LIMITS };
    console.log(`${tag} active`);
    return socket.bugShield;
}

/** Process level safety net: unhandled rejection awath process eka crash wenna epa. */
export function installProcessGuards(onFatal) {
    if (globalThis.__bugGuardsInstalled) return;
    globalThis.__bugGuardsInstalled = true;
    const hits = [];
    process.on('unhandledRejection', (reason) => {
        console.error('[BugShield] unhandledRejection:', reason?.message || reason);
    });
    process.on('uncaughtException', (err) => {
        console.error('[BugShield] uncaughtException:', err?.message || err);
        const now = Date.now();
        hits.push(now);
        while (hits.length && now - hits[0] > 30_000) hits.shift();
        // restart loop ekak wenna ba: 30s athule 5k+ unoth witharai restart
        if (hits.length >= 5) { hits.length = 0; try { onFatal?.(err); } catch { /* ignore */ } }
    });
}
