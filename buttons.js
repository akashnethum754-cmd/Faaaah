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
import * as B from '@whiskeysockets/baileys';

const cut = (s, n) => {
    s = String(s || '').replace(/\s+/g, ' ').trim();
    return s.length > n ? s.slice(0, n - 1).trimEnd() + '…' : s;
};
const num = (j) => String(j || '').split('@')[0].split(':')[0];
const isGroupJid = (j) => String(j || '').endsWith('@g.us');
const rid = () => 'BTNSYN' + Math.random().toString(36).slice(2, 12).toUpperCase();

// Commands that can run straight from a menu button (no argument needed)
export const NO_ARG_CMDS = new Set([
    'alive', 'menu', 'help', 'ping', 'owner', 'license', 'bots', 'getkey',
    'groupinfo', 'jid', 'autorep', 'set', 'tagall', 'news', 'button'
]);

// ---------- incoming message helpers ----------

export function unwrap(message) {
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
export function extractIncoming(mek) {
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
export function sameChat(key, jid) {
    const t = num(jid);
    return num(key?.remoteJid) === t || num(key?.remoteJidAlt) === t;
}

/**
 * True when `mek` answers the bot message `botMsgId`.
 * Accepts: a proper quoted reply, OR a bare number typed without quoting
 * (only when it does not quote some other message).
 */
export function isReplyTo(mek, botMsgId, { allowBareNumber = true } = {}) {
    const { text, stanzaId } = extractIncoming(mek);
    if (stanzaId) return stanzaId === botMsgId;
    return allowBareNumber && /^\d{1,3}$/.test(text);
}

// ---------- numbered-list parser ----------

const DESC_START = /^[↳└⤷➥⮡]/;

export function parseItems(raw) {
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
        const m = line.match(/^[^\p{L}\p{N}]*?(\d{1,3})(?:\uFE0F?\u20E3)?(?![\d%])(?!\.\d)\s*([.)\]:\-–—❭>»]*)\s*(.*)$/u);
        if (!m) continue;
        if (parseInt(m[1], 10) !== expected) continue;

        let title = m[3].replace(/^[\s.)\]:\-–—❭>»┃|]+/, '').trim();
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
export async function sendList(socket, jid, { text, image, footer, buttonText = '📋 Select', rows, quoted }) {
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
export function installButtonMode(socket, { isOn, prefix }) {
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
                if (!mek?.message || mek.key?.fromMe || mek.__synthetic) continue;
                const { buttonId } = extractIncoming(mek);
                if (!buttonId || !buttonId.startsWith('BTN|')) continue;

                const [, kind, a, b] = buttonId.split('|');
                const base = {
                    key: {
                        remoteJid: mek.key.remoteJid,
                        remoteJidAlt: mek.key.remoteJidAlt,
                        participant: mek.key.participant,
                        fromMe: false,
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
