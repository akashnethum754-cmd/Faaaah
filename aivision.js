// 🖼️ AI ECO VISION + IMAGE GENERATION helpers
//  - ecoReadImage(): contact eth awapu photo eka AI ekata balanna (data URI, podi karala)
//  - ecoImageIntent(): "cat image ekak hadanna" wage message eken prompt eka ganna
//  - ecoGenImage(): AI image generate karala URL eka denawa
//  - ecoImagePrompt(): Sinhala / Singlish request eka English image prompt ekak karanawa

import axios from 'axios';
import sharp from 'sharp';

const IMG_API = process.env.KCEY_IMG_URL || 'https://api.kcey.online/api/aiimage';
const IMG_KEY = process.env.KCEY_API_KEY || 'kcey_5e4ab9c59757bf32a90f237e31ff30b0d9a5';

const MAX_DOWNLOAD = 12 * 1024 * 1024;

// explicit / abusive image requests: generate karanne na
export const ECO_IMG_BLOCK = /\b(nude|nudes|naked|porn|porno|sex|sexy|xxx|nsfw|boobs|pussy|dick|cock|hentai|18\+|lingerie|bikini)\b|නිරුවත්|සෙක්ස්|පොර්න්/i;

const NOUN = /\b(image|images|img|photo|photos|pic|pics|picture|pictures|wallpaper|logo|poster|artwork|illustration|pinthuraya|pinthurayak|pintoorayak|pintooraya)\b|පින්තූර\S*|රූපයක්|රූපය|ෆොටෝ\S*|ලෝගෝ\S*/iu;
const VERB = /\b(hadala|hadalaa|hadanna|hadapan|hadapn|hadagena|hadaganna|generate|create|make|draw|design|sketch|paint|aanda|andapan|andala|andanna)\b|හදලා|හදන්න|හදපන්|ඇඳලා|අඳින්න|ඇඳපන්/iu;
const FILL = /\b(ekak|ekk|ek|ekata|eka|one|oni|ona|mata|denna|dennako|dapan|plz|pls|please|kiyala|ewanna|ewapan)\b/gi;

/** @returns null | {type:'image',prompt} | {type:'ask',what:'image',kind:'image'} | {type:'image_blocked'} */
export function ecoImageIntent(textRaw) {
    const text = String(textRaw || '').trim();
    if (!text || text.length > 300) return null;
    if (!NOUN.test(text) || !VERB.test(text)) return null;
    if (ECO_IMG_BLOCK.test(text)) return { type: 'image_blocked' };
    const prompt = text
        .replace(new RegExp(NOUN.source, 'giu'), ' ')
        .replace(new RegExp(VERB.source, 'giu'), ' ')
        .replace(FILL, ' ')
        .replace(/[^\p{L}\p{N}\p{M}\u200D\u200C\s,'-]/gu, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    if (prompt.length < 2) return { type: 'ask', what: 'image', kind: 'image' };
    return { type: 'image', prompt: prompt.slice(0, 250) };
}

/** Photo eka AI ekata yawanna puluwan data URI ekak widihata (max 1024px JPEG) */
export async function ecoReadImage(m, downloadMediaMessage) {
    const im = m?.message?.imageMessage;
    if (!im) return null;
    if (Number(im.fileLength || 0) > MAX_DOWNLOAD) return null;
    let buf = await downloadMediaMessage(m, 'buffer', {}, { logger: console });
    if (!buf?.length || buf.length > MAX_DOWNLOAD) return null;
    try {
        buf = await sharp(buf).rotate()
            .resize({ width: 1024, height: 1024, fit: 'inside', withoutEnlargement: true })
            .jpeg({ quality: 78 }).toBuffer();
        return `data:image/jpeg;base64,${buf.toString('base64')}`;
    } catch {
        return `data:${im.mimetype || 'image/jpeg'};base64,${buf.toString('base64')}`;
    }
}

/** Sinhala / Singlish request -> English image prompt (ask = AI text function) */
export async function ecoImagePrompt(raw, ask) {
    const s = String(raw || '').trim().slice(0, 300);
    if (!s) return s;
    const english = /^[\x00-\x7F]+$/.test(s) && !/\b(mata|oya|ekak|ekata|hadala|kiyala|wage|eka)\b/i.test(s);
    if (english) return s;
    try {
        const out = await ask(`Rewrite this image request as ONE short, vivid English prompt for an AI image generator. Output ONLY the prompt, nothing else.\nRequest: ${s}`);
        if (out && out.length >= 3 && !ECO_IMG_BLOCK.test(out)) return out.slice(0, 300);
    } catch { /* fallback raw */ }
    return s;
}

/** Image generate karala { image, width, height, model, final_prompt } denawa */
export async function ecoGenImage(prompt) {
    if (ECO_IMG_BLOCK.test(prompt)) throw new Error('blocked prompt');
    const res = await axios.get(IMG_API, { params: { q: prompt, key: IMG_KEY }, timeout: 120000 });
    const d = res.data;
    if (!d?.ok || !d.image) throw new Error(d?.error || 'image generate failed');
    return d;
}
