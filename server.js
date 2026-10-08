// ===== سوقي — موقع إعلانات مبوبة عربي | Node.js 18+ | بدون أي مكتبات خارجية =====
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto'),zlib=require('zlib'),os=require('os');
const PORT=process.env.PORT||3000,DATA=process.env.DATA_DIR||process.env.RAILWAY_VOLUME_MOUNT_PATH||path.join(__dirname,'data'),UP=path.join(DATA,'uploads'),DBF=path.join(DATA,'db.json'),DBS=path.join(DATA,'souqi.db');
const SITE=process.env.SITE_NAME||'سوقي',PER=20;
fs.mkdirSync(UP,{recursive:true});
const VOL=process.env.RAILWAY_VOLUME_MOUNT_PATH,EPH=!!(process.env.RAILWAY_ENVIRONMENT&&!(VOL&&path.resolve(DATA).startsWith(path.resolve(VOL))));
if(EPH)console.warn('\n⚠️⚠️ تحذير: الموقع شغال على Railway بدون Volume دائم — البيانات هتتمسح مع كل تحديث! اعمل Volume واربطه بالمسار /data وضيف DATA_DIR=/data\n');
const CATS=[['cars','سيارات','🚗'],['real-estate','عقارات','🏠'],['mobiles','موبايلات وتابلت','📱'],['electronics','إلكترونيات','💻'],['furniture','أثاث ومنزل','🛋️'],['jobs','وظائف','💼'],['services','خدمات','🛠️'],['fashion','ملابس وموضة','👗']];
const COUNTRIES=[['eg','مصر','ج.م','EGP'],['sa','السعودية','ر.س','SAR'],['ae','الإمارات','د.إ','AED'],['kw','الكويت','د.ك','KWD'],['qa','قطر','ر.ق','QAR'],['bh','البحرين','د.ب','BHD'],['om','عُمان','ر.ع','OMR'],['jo','الأردن','د.أ','JOD'],['lb','لبنان','ل.ل','LBP'],['iq','العراق','د.ع','IQD'],['ma','المغرب','د.م','MAD'],['dz','الجزائر','د.ج','DZD'],['tn','تونس','د.ت','TND'],['ly','ليبيا','د.ل','LYD'],['sd','السودان','ج.س','SDG'],['ye','اليمن','ر.ي','YER'],['sy','سوريا','ل.س','SYP'],['ps','فلسطين','₪','ILS']];
const CM=Object.fromEntries(CATS.map(c=>[c[0],c])),KM=Object.fromEntries(COUNTRIES.map(c=>[c[0],c]));

// ---------- قاعدة البيانات: SQLite (مدمجة في Node 22 — بدون أي مكتبات) ----------
const {DatabaseSync}=require('node:sqlite');
const SQL=new DatabaseSync(DBS);
SQL.exec(`PRAGMA journal_mode=WAL;PRAGMA synchronous=NORMAL;PRAGMA busy_timeout=5000;PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY,v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY,j TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS ads(id INTEGER PRIMARY KEY,j TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY,j TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS banners(id INTEGER PRIMARY KEY,j TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sess(k TEXT PRIMARY KEY,j TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS convs(id INTEGER PRIMARY KEY AUTOINCREMENT,ad_id INTEGER NOT NULL DEFAULT 0,buyer_id INTEGER NOT NULL,seller_id INTEGER NOT NULL,created INTEGER NOT NULL,last_at INTEGER NOT NULL,UNIQUE(ad_id,buyer_id,seller_id));
CREATE TABLE IF NOT EXISTS msgs(id INTEGER PRIMARY KEY AUTOINCREMENT,conv_id INTEGER NOT NULL REFERENCES convs(id) ON DELETE CASCADE,sender_id INTEGER NOT NULL,body TEXT NOT NULL,created INTEGER NOT NULL,read_at INTEGER);
CREATE INDEX IF NOT EXISTS msgs_conv ON msgs(conv_id,id);
CREATE INDEX IF NOT EXISTS convs_buyer ON convs(buyer_id);
CREATE INDEX IF NOT EXISTS convs_seller ON convs(seller_id);`);
const q1=(s,...a)=>SQL.prepare(s).get(...a),qa=(s,...a)=>SQL.prepare(s).all(...a),qr=(s,...a)=>SQL.prepare(s).run(...a);
let db={users:[],ads:[],sess:{},next:1,nextAd:1,admin:null,orders:[],nextOrder:1,banners:[],nextBanner:1,settings:{}},dirty=false;
const COLS=['users','ads','orders','banners'],METAK=['next','nextAd','nextOrder','nextBanner','admin','settings'],snap={sess:new Map(),meta:new Map()};
for(const k of COLS)snap[k]=new Map();
function loadDb(){
  for(const k of COLS){db[k]=qa(`SELECT j FROM ${k}`).map(r=>JSON.parse(r.j));snap[k]=new Map(db[k].map(x=>[x.id,JSON.stringify(x)]))}
  db.sess={};snap.sess=new Map();for(const r of qa('SELECT k,j FROM sess')){db.sess[r.k]=JSON.parse(r.j);snap.sess.set(r.k,r.j)}
  snap.meta=new Map();for(const r of qa('SELECT k,v FROM meta')){snap.meta.set(r.k,r.v);if(METAK.includes(r.k))db[r.k]=JSON.parse(r.v)}
}
// الحفظ: يكتب فقط الصفوف اللي اتغيّرت، داخل transaction واحدة (يا كله يتحفظ يا ولا حاجة)
function save(){
  SQL.exec('BEGIN');
  try{
    for(const k of COLS){const m=snap[k],seen=new Set(),up=SQL.prepare(`INSERT INTO ${k}(id,j) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET j=excluded.j`);
      for(const x of db[k]){const s=JSON.stringify(x);seen.add(x.id);if(m.get(x.id)!==s){up.run(x.id,s);m.set(x.id,s)}}
      const del=SQL.prepare(`DELETE FROM ${k} WHERE id=?`);for(const id of[...m.keys()])if(!seen.has(id)){del.run(id);m.delete(id)}}
    {const m=snap.sess,up=SQL.prepare('INSERT INTO sess(k,j) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET j=excluded.j'),del=SQL.prepare('DELETE FROM sess WHERE k=?');
      for(const k in db.sess){const s=JSON.stringify(db.sess[k]);if(m.get(k)!==s){up.run(k,s);m.set(k,s)}}
      for(const k of[...m.keys()])if(!(k in db.sess)){del.run(k);m.delete(k)}}
    {const m=snap.meta,up=SQL.prepare('INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v');
      for(const k of METAK){const s=JSON.stringify(db[k]??null);if(m.get(k)!==s){up.run(k,s);m.set(k,s)}}}
    SQL.exec('COMMIT');
  }catch(e){try{SQL.exec('ROLLBACK')}catch{}for(const k of[...COLS,'sess','meta'])snap[k].clear();throw e}
  dirty=false;
}
loadDb();
// نقل تلقائي لمرة واحدة من النسخة القديمة (db.json) لو موجودة
if(!snap.meta.has('migrated')){
  if(fs.existsSync(DBF)){try{const old=JSON.parse(fs.readFileSync(DBF,'utf8'));db={...db,...old};save();fs.renameSync(DBF,DBF+'.migrated');console.log('✅ تم نقل بياناتك القديمة من db.json إلى قاعدة البيانات (النسخة القديمة اتحفظت باسم db.json.migrated)')}catch(e){console.error('فشل نقل db.json:',e.message);process.exit(1)}}
  SQL.prepare("INSERT OR REPLACE INTO meta(k,v) VALUES('migrated','1')").run();snap.meta.set('migrated','1');
}
for(const a of db.ads)if(!CM[a.cat]){a.cat='services';dirty=true}
setInterval(()=>{if(dirty)try{save()}catch(e){console.error(e)}},15000);
for(const s of['SIGTERM','SIGINT'])process.on(s,()=>{try{save();SQL.close()}catch{}process.exit(0)});
const hash=(p,s)=>crypto.scryptSync(p,s,32).toString('hex'),sha=s=>crypto.createHash('sha256').update(s).digest('hex');
for(const k in db.sess)if(db.sess[k].exp<Date.now())delete db.sess[k];
if(!db.nextAd||db.ads.some(a=>a.id>=db.nextAd))db.nextAd=Math.max(0,...db.ads.map(a=>a.id))+1;
(function initAdmin(){const u=process.env.ADMIN_USER||'admin';let p=process.env.ADMIN_PASS;if(!p&&db.admin)return;
  if(!p){p=crypto.randomBytes(9).toString('base64url');console.log(`\n*** بيانات الأدمن: المستخدم=${u} | كلمة المرور=${p} | (لتحديد كلمة مرور خاصة ضع ADMIN_PASS في المتغيرات) ***\n`)}
  const salt=crypto.randomBytes(16).toString('hex');db.admin={user:u,salt,hash:hash(p,salt)};save()})();

// ---------- أدوات ----------
const E=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const H=u=>E(encodeURI(u));
const slug=t=>String(t).trim().replace(/[^\p{L}\p{N}]+/gu,'-').replace(/^-|-$/g,'').slice(0,60)||'ad';
const adUrl=a=>`/ad/${a.id}-${slug(a.title)}`;
const norm=s=>String(s||'').toLowerCase().replace(/[\u064B-\u065F\u0640]/g,'').replace(/[أإآ]/g,'ا').replace(/ى/g,'ي').replace(/ة/g,'ه');
const nf=new Intl.NumberFormat('en-US');
const price=a=>a.price>0?`${nf.format(a.price)} ${KM[a.country][2]}`:'السعر بالاتفاق';
const locOf=a=>a.city?`${a.city}، ${KM[a.country][1]}`:KM[a.country][1];
const when=t=>{const d=new Date(t),n=new Date(),dd=Math.floor((new Date(n.getFullYear(),n.getMonth(),n.getDate())-new Date(d.getFullYear(),d.getMonth(),d.getDate()))/864e5);return dd<=0?'اليوم':dd===1?'أمس':d.toLocaleDateString('ar-EG',{day:'numeric',month:'long',year:'numeric'})};
const tries=new Map(),limited=(k,max,ms)=>{const t=tries.get(k)||{n:0,until:0};if(t.until>Date.now())return true;t.n++;if(t.n>=max){t.n=0;t.until=Date.now()+ms}tries.set(k,t);return false};
const raw=(req,max)=>new Promise((ok,no)=>{const ch=[];let n=0,big=false;req.on('data',d=>{n+=d.length;if(n>max)big=true;else ch.push(d)});req.on('end',()=>big?no(new Error('big')):ok(Buffer.concat(ch)));req.on('error',no)});
const form=async req=>Object.fromEntries(new URLSearchParams((await raw(req,2e5).catch(()=>Buffer.alloc(0))).toString()));
const ck=(req,n)=>(req.headers.cookie||'').split(';').map(s=>s.trim().split('=')).find(x=>x[0]===n)?.[1];
const getUser=req=>{const s=db.sess[sha(ck(req,'sid')||'')];return s&&s.exp>Date.now()&&s.k==='u'?db.users.find(u=>u.id===s.uid)||null:null};
const isAdmin=req=>{const s=db.sess[sha(ck(req,'asid')||'')];return !!(s&&s.exp>Date.now()&&s.k==='a')};
const https=req=>req.headers['x-forwarded-proto']==='https';
const startSess=(req,k,uid)=>{const t=crypto.randomBytes(24).toString('hex'),a=k==='a';db.sess[sha(t)]={k,uid,exp:Date.now()+(a?288e5:2592e6)};save();return`${a?'asid':'sid'}=${t}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${a?28800:2592000}${https(req)?'; Secure':''}`};
const endSess=(req,n)=>{const t=ck(req,n);if(t){delete db.sess[sha(t)];save()}return`${n}=; Max-Age=0; Path=/`};
const okOrigin=req=>{const o=req.headers.origin||req.headers.referer;if(!o)return true;try{const h=new URL(o).host;return h===req.headers.host||h===req.headers['x-forwarded-host']}catch{return false}};
const safeNext=n=>typeof n==='string'&&/^\/[^\/\\]/.test(n)?n:'/';
const hits=new Map(),rate=(k,max,ms)=>{const n=Date.now(),a=(hits.get(k)||[]).filter(t=>n-t<ms);if(a.length>=max){hits.set(k,a);return true}a.push(n);hits.set(k,a);return false};
setInterval(()=>{const n=Date.now();for(const[k,a]of hits)if(!a.length||n-a[a.length-1]>36e5)hits.delete(k)},6e5).unref();
const unread=uid=>uid?q1('SELECT COUNT(*) n FROM msgs m JOIN convs c ON c.id=m.conv_id WHERE (c.buyer_id=? OR c.seller_id=?) AND m.sender_id!=? AND m.read_at IS NULL',uid,uid,uid).n:0;
const roleOf=u=>u&&u.role==='buyer'?'buyer':'seller';
const sellerUrl=u=>`/seller/${u.id}-${slug(u.name)}`;
const initial=n=>[...String(n||'?').trim()][0]||'?';

// ---------- الشكل (CSS + لوجو) ----------
const LOGO='<svg viewBox="0 0 40 40" width="38" height="38" aria-hidden="true"><rect width="40" height="40" rx="10" fill="#0f766e"/><path d="M11 15h18l-1.6 14a2 2 0 0 1-2 1.8H14.6a2 2 0 0 1-2-1.8z" fill="#fff"/><path d="M15.5 15v-2.5a4.5 4.5 0 0 1 9 0V15" fill="none" stroke="#fbbf24" stroke-width="2.4" stroke-linecap="round"/></svg>';
const CSS=`:root{--p:#0f766e;--pd:#0b4f4a;--a:#f59e0b;--bg:#f4f6f8;--tx:#1f2937;--m:#6b7280;--bd:#e5e7eb}
*{box-sizing:border-box;margin:0}body{font-family:"Segoe UI",Tahoma,Arial,sans-serif;background:var(--bg);color:var(--tx);line-height:1.7;font-size:16px}
a{color:inherit;text-decoration:none}img{max-width:100%}.w{max-width:1150px;margin:auto;padding:0 14px}
header{background:#fff;border-bottom:1px solid var(--bd);position:sticky;top:0;z-index:9}.bar{display:flex;align-items:center;gap:14px;padding:10px 14px;flex-wrap:wrap}
.logo{display:flex;align-items:center;gap:8px;font-size:22px;font-weight:800;color:var(--pd)}.bar nav{display:flex;gap:14px;flex:1;font-size:15px;color:var(--m)}.bar nav a:hover{color:var(--p)}
.acts{display:flex;align-items:center;gap:12px;font-size:14px}.acts form{display:inline}
.btn,button.btn{background:var(--p);color:#fff;border:0;padding:10px 20px;border-radius:10px;font:inherit;font-weight:700;cursor:pointer;display:inline-block;text-align:center}.btn:hover{background:var(--pd)}.btn.acc{background:var(--a);color:#3b2a00}.btn.acc:hover{background:#d98a06;color:#fff}
.btn.sm{padding:5px 12px;font-size:13px}.btn.red{background:#dc2626}.lnk{background:none;border:0;font:inherit;color:var(--m);cursor:pointer}.lnk:hover{color:var(--p)}
.hero{background:linear-gradient(135deg,#0b4f4a,#0f766e 60%,#14b8a6);color:#fff;padding:44px 0 50px;text-align:center}.hero h1{font-size:clamp(24px,5vw,38px);margin-bottom:8px}.hero p{opacity:.9;margin-bottom:22px}
.sf{display:flex;gap:8px;flex-wrap:wrap;justify-content:center;background:#fff;padding:10px;border-radius:14px;max-width:820px;margin:auto}.sf input,.sf select{flex:1;min-width:130px;padding:12px;border:1px solid var(--bd);border-radius:10px;font:inherit}
h2{font-size:21px;margin:30px 0 14px}h2 small{color:var(--m);font-weight:400;font-size:14px}
.cats{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:12px}.cat{background:#fff;border:1px solid var(--bd);border-radius:14px;padding:16px 8px;text-align:center;font-weight:600;transition:.15s}.cat:hover{border-color:var(--p);transform:translateY(-2px)}.cat span{display:block;font-size:30px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:14px}.card{background:#fff;border:1px solid var(--bd);border-radius:14px;overflow:hidden;transition:.15s;display:block}.card:hover{box-shadow:0 8px 22px #0001;transform:translateY(-2px)}
.card .im{position:relative;aspect-ratio:4/3;background:#e6f4f1;display:flex;align-items:center;justify-content:center;font-size:48px}.card .im img{width:100%;height:100%;object-fit:cover}.card .sb{position:absolute;top:8px;right:8px;background:#dc2626;color:#fff;font-size:12.5px;font-weight:700;padding:2px 10px;border-radius:20px;z-index:1}.cb{padding:12px}.cb h3{font-size:15px;font-weight:600;margin:4px 0;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}.cb small{color:var(--m);font-size:12.5px}.pr{color:var(--p);font-size:17px}
.f{background:#fff;border:1px solid var(--bd);border-radius:14px;padding:22px;display:grid;gap:14px;max-width:640px;margin:24px auto}.f label{display:grid;gap:5px;font-size:14px;font-weight:600}.f input,.f select,.f textarea{padding:11px;border:1px solid #cbd5e1;border-radius:10px;font:inherit;font-weight:400;width:100%}.f input:focus,.f select:focus,.f textarea:focus,.sf input:focus{outline:2px solid var(--p);border-color:var(--p)}
.row{display:grid;grid-template-columns:1fr 1fr;gap:12px}.er{background:#fee2e2;color:#991b1b;padding:10px 14px;border-radius:10px}.ok{background:#d1fae5;color:#065f46;padding:10px 14px;border-radius:10px}
#prev{display:flex;gap:8px;flex-wrap:wrap}#prev img{width:80px;height:80px;object-fit:cover;border-radius:8px}
.bc{font-size:13.5px;color:var(--m);margin:16px 0 4px}.bc a:hover{color:var(--p)}.pg{display:flex;gap:6px;justify-content:center;margin:26px 0;flex-wrap:wrap}.pg a,.pg b{padding:7px 14px;border-radius:8px;background:#fff;border:1px solid var(--bd)}.pg b{background:var(--p);color:#fff;border-color:var(--p)}
.ad{display:grid;grid-template-columns:1fr 330px;gap:20px;align-items:start}.box{background:#fff;border:1px solid var(--bd);border-radius:14px;padding:18px}.gal{display:grid;gap:10px;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));margin-bottom:14px}.gal img{width:100%;border-radius:12px;max-height:480px;object-fit:cover}
.big{font-size:26px;color:var(--p);font-weight:800}.desc{white-space:pre-wrap;word-break:break-word}.tag{background:#e6f4f1;color:var(--pd);padding:2px 10px;border-radius:20px;font-size:13px}.sold{background:#fee2e2;color:#991b1b}
table{width:100%;border-collapse:collapse;background:#fff;border-radius:12px;overflow:hidden}th,td{padding:10px;border-bottom:1px solid var(--bd);text-align:right;font-size:14px}th{background:#f1f5f9}.stat{display:flex;gap:14px;flex-wrap:wrap}.stat div{background:#fff;border:1px solid var(--bd);border-radius:12px;padding:14px 22px;font-size:26px;font-weight:800;color:var(--p)}.stat small{display:block;font-size:13px;color:var(--m);font-weight:400}
.empty{text-align:center;background:#fff;border:1px dashed #cbd5e1;border-radius:14px;padding:40px 14px}footer{background:var(--pd);color:#cde;margin-top:44px;padding:28px 0;font-size:14px}footer a{margin:0 8px}footer .w>div{margin-bottom:10px}
.adv{display:block;position:relative;margin:10px auto;max-width:728px;max-height:90px;overflow:hidden;text-align:center;border-radius:8px}.adv img{display:block;margin:auto;max-height:90px;max-width:100%;width:auto}.adv small{position:absolute;top:2px;left:4px;background:#0007;color:#fff;font-size:10px;padding:0 6px;border-radius:4px;line-height:16px}.adv.side{max-width:100%;max-height:250px;margin:14px 0 0}.adv.side img{max-height:250px}
.f .pm{display:grid;gap:8px}.f .pm label{display:flex;align-items:center;gap:10px;border:1px solid var(--bd);padding:10px 12px;border-radius:10px;cursor:pointer}.f .pm input{width:auto}
.succ{text-align:center;background:#fff;border:1px solid #a7f3d0;border-radius:14px;padding:28px 14px;margin:24px auto;max-width:640px}.succ .ic{font-size:52px}
.thumb{width:54px;height:42px;object-fit:cover;border-radius:6px;display:block}.st{display:inline-block;padding:2px 10px;border-radius:20px;font-size:12.5px;background:#f1f5f9;white-space:nowrap}.st.g{background:#d1fae5;color:#065f46}.st.y{background:#fef3c7;color:#92400e}.st.r{background:#fee2e2;color:#991b1b}.tbw{overflow-x:auto;border-radius:12px}.tbw table{min-width:640px}td small{color:var(--m)}
.bdg{background:#dc2626;color:#fff;border-radius:20px;padding:0 7px;font-size:12px;font-weight:700}
.sel{display:flex;gap:16px;align-items:center;flex-wrap:wrap}.av{width:72px;height:72px;border-radius:50%;background:var(--p);color:#fff;display:flex;align-items:center;justify-content:center;font-size:32px;font-weight:800;flex:none}.av.sm{width:40px;height:40px;font-size:18px}
.sst{display:flex;gap:18px;flex-wrap:wrap;color:var(--m);font-size:14px;margin-top:6px}.sst b{color:var(--tx)}.pmb{display:inline-block;background:#e6f4f1;color:var(--pd);padding:3px 12px;border-radius:20px;font-size:13px;margin:2px}
.pmf{border:1px solid var(--bd);border-radius:10px;padding:12px;display:grid;gap:10px}.pmf legend{font-weight:700;padding:0 6px}
.chat{max-width:760px;margin:18px auto 10px;display:flex;flex-direction:column;height:calc(100vh - 170px);min-height:420px}
.chh{display:flex;gap:12px;align-items:center;background:#fff;border:1px solid var(--bd);border-radius:14px 14px 0 0;padding:12px 14px}.chh .ctx{margin-inline-start:auto;font-size:13px;color:var(--m)}
.msgs{flex:1;overflow-y:auto;background:#eef2f5;border-inline:1px solid var(--bd);padding:14px;display:flex;flex-direction:column;gap:8px}
.msg{max-width:78%;padding:8px 12px;border-radius:14px;line-height:1.6;word-break:break-word}.msg p{white-space:pre-wrap}.msg small{display:block;font-size:11px;opacity:.7;margin-top:2px}
.msg.me{align-self:flex-start;background:var(--p);color:#fff;border-end-start-radius:4px}.msg.them{align-self:flex-end;background:#fff;border:1px solid var(--bd);border-end-end-radius:4px}
.cf{display:flex;gap:8px;background:#fff;border:1px solid var(--bd);border-radius:0 0 14px 14px;padding:10px}.cf textarea{flex:1;padding:10px;border:1px solid var(--bd);border-radius:10px;font:inherit;resize:none}
.cl a{display:flex;gap:12px;align-items:center;background:#fff;border:1px solid var(--bd);border-radius:14px;padding:12px 14px;margin-bottom:10px}.cl a:hover{border-color:var(--p)}.cl .tx{flex:1;min-width:0}.cl .tx p{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--m);font-size:14px}
.warn{background:#fef3c7;color:#92400e;padding:10px 14px;border-radius:10px;font-size:14px;max-width:760px;margin:0 auto 14px}
@media(max-width:760px){.ad{grid-template-columns:1fr}.bar nav{display:none}.row{grid-template-columns:1fr}}`;
const CSSV=sha(CSS).slice(0,8);

// ---------- قالب الصفحة + SEO ----------
function page(c,o,main){
  const {b,user}=c,url=b+encodeURI(o.path||c.p),un=user?unread(user.id):0;
  const title=o.home?`${SITE} - إعلانات مبوبة مجانية، بيع واشتري في الوطن العربي`:`${o.title} | ${SITE}`;
  const desc=E(String(o.desc||'').replace(/\s+/g,' ').slice(0,158));
  const ld=(o.ld||[]).map(x=>`<script type="application/ld+json">${JSON.stringify(x).replace(/</g,'\\u003c')}</script>`).join('');
  const img=o.image?`<meta property="og:image" content="${E(o.image)}"><meta name="twitter:image" content="${E(o.image)}">`:'';
  return`<!doctype html><html lang="ar" dir="rtl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#0f766e">
<title>${E(title)}</title><meta name="description" content="${desc}"><meta name="robots" content="${o.noindex?'noindex,follow':'index,follow,max-image-preview:large'}"><link rel="canonical" href="${E(url)}">
<meta property="og:site_name" content="${E(SITE)}"><meta property="og:locale" content="ar_AR"><meta property="og:type" content="${o.type||'website'}"><meta property="og:title" content="${E(title)}"><meta property="og:description" content="${desc}"><meta property="og:url" content="${E(url)}">${img}
<meta name="twitter:card" content="${o.image?'summary_large_image':'summary'}"><meta name="twitter:title" content="${E(title)}"><meta name="twitter:description" content="${desc}">
<link rel="icon" href="/favicon.svg" type="image/svg+xml"><link rel="stylesheet" href="/style.css?v=${CSSV}">${ld}</head><body>
<header><div class="w bar"><a class="logo" href="/" aria-label="${E(SITE)}">${LOGO}<span>${E(SITE)}</span></a><nav aria-label="القائمة"><a href="/ads">كل الإعلانات</a><a href="/about">من نحن</a></nav>
<div class="acts">${user?`<a href="/dashboard">${roleOf(user)==='seller'?'لوحتي':'حسابي'}</a><a href="/my-orders">طلباتي</a><a href="/chats">💬 الرسائل${un?` <span class="bdg">${un}</span>`:''}</a>${roleOf(user)==='seller'?`<a href="${H(sellerUrl(user))}">صفحتي</a>`:''}<form method="post" action="/logout"><button class="lnk">خروج</button></form>`:`<a href="/login">دخول</a><a href="/register">حساب جديد</a>`}<a class="btn acc sm" href="/post">+ أضف إعلان</a></div></div></header>
${o.noindex?'':bn('top')}
${main}
<footer><div class="w"><div>${CATS.map(x=>`<a href="/c/${x[0]}">${x[1]}</a>`).join('')}</div><div><a href="/">الرئيسية</a><a href="/ads">كل الإعلانات</a><a href="/about">من نحن</a><a href="/post">أضف إعلان</a></div><div>© ${new Date().getFullYear()} ${E(SITE)} — جميع الحقوق محفوظة</div></div></footer></body></html>`;
}
const card=a=>`<a class="card" href="${H(adUrl(a))}"><div class="im">${a.status==='sold'?'<span class="sb">تم البيع</span>':''}${a.images[0]?`<img src="${E(a.images[0])}" alt="${E(a.title)}" loading="lazy" width="400" height="300">`:CM[a.cat][2]}</div><div class="cb"><b class="pr">${E(price(a))}</b><h3>${E(a.title)}</h3><small>${E(locOf(a))} • ${E(when(a.created))}</small></div></a>`;
const opts=(arr,sel,all)=>(all?`<option value="">${all}</option>`:'')+arr.map(x=>`<option value="${x[0]}"${x[0]===sel?' selected':''}>${E(x[1])}</option>`).join('');
const pager=(base,q,pg,pages)=>{if(pages<2)return'';const mk=n=>{const s=new URLSearchParams(q);if(n>1)s.set('page',n);const t=s.toString();return base+(t?'?'+t:'')};
  let h='<nav class="pg" aria-label="الصفحات">';if(pg>1)h+=`<a rel="prev" href="${E(mk(pg-1))}">السابق</a>`;
  for(let i=Math.max(1,pg-2);i<=Math.min(pages,pg+2);i++)h+=i===pg?`<b>${i}</b>`:`<a href="${E(mk(i))}">${i}</a>`;
  if(pg<pages)h+=`<a rel="next" href="${E(mk(pg+1))}">التالي</a>`;return h+'</nav>'};
const bcLd=(b,items)=>({'@context':'https://schema.org','@type':'BreadcrumbList',itemListElement:items.map((x,i)=>({'@type':'ListItem',position:i+1,name:x[0],item:b+encodeURI(x[1])}))});

// ---------- الاستجابات ----------
function out(c,code,body,type='text/html; charset=utf-8',extra={}){const h={'Content-Type':type,Vary:'Accept-Encoding',...extra};
  if(Buffer.byteLength(body)>1024&&/gzip/.test(c.req.headers['accept-encoding']||'')){h['Content-Encoding']='gzip';body=zlib.gzipSync(body)}c.res.writeHead(code,h);c.res.end(body)}
const html=(c,code,o,main)=>out(c,code,page(c,o,main));
const redirect=(c,loc,cookies,code=303)=>{const h={Location:encodeURI(loc)};if(cookies)h['Set-Cookie']=cookies;c.res.writeHead(code,h);c.res.end()};
const nf404=c=>html(c,404,{title:'الصفحة غير موجودة',noindex:1,path:'/404'},`<main class="w"><div class="empty" style="margin-top:30px"><h1>404</h1><p>الصفحة المطلوبة غير موجودة.</p><p><a class="btn" href="/">العودة للرئيسية</a></p></div></main>`);
const json=(c,code,o)=>out(c,code,JSON.stringify(o),'application/json; charset=utf-8',{'Cache-Control':'no-store'});

// ---------- الصفحات ----------
function listPage(c,fixedCat){
  const sp=c.u.searchParams,q=(sp.get('q')||'').trim().slice(0,60),cat=fixedCat||(CM[sp.get('cat')]?sp.get('cat'):'');
  if(!fixedCat&&cat){const s=new URLSearchParams(sp);s.delete('cat');return redirect(c,`/c/${cat}${s.toString()?'?'+s:''}`,null,301)}
  const l=db.ads.filter(a=>['active','sold'].includes(a.status)&&(!cat||a.cat===cat)&&(!q||norm(a.title+' '+a.desc).includes(norm(q)))).sort((a,b)=>(b.status==='active')-(a.status==='active')||b.created-a.created);
  const pages=Math.max(1,Math.ceil(l.length/PER)),pg=Math.min(Math.max(1,parseInt(sp.get('page'))||1),pages),base=fixedCat?`/c/${fixedCat}`:'/ads';
  const name=cat?`${CM[cat][1]} للبيع`:'كل الإعلانات',t=q?`نتائج البحث عن ${q}`:name+(pg>1?` - صفحة ${pg}`:'');
  const keep=new URLSearchParams();if(q)keep.set('q',q);
  const cq=new URLSearchParams();if(pg>1)cq.set('page',pg);
  const bc=[['الرئيسية','/']];if(cat)bc.push([CM[cat][1],base]);
  html(c,200,{title:t,desc:`تصفح ${l.length} إعلان ${cat?CM[cat][1]:'مبوب'}على ${SITE}. أحدث العروض بأسعار مناسبة وتواصل مباشر مع البائعين.`,path:base+(cq.toString()?'?'+cq:''),noindex:!!q,ld:[bcLd(c.b,bc)]},
  `<main class="w"><p class="bc"><a href="/">الرئيسية</a>${cat?` › <a href="${base}">${E(CM[cat][1])}</a>`:''}</p><h1 style="font-size:26px">${E(t)}</h1>
<form class="sf" action="${base}" style="margin:14px 0;max-width:none"><input name="q" value="${E(q)}" placeholder="ابحث..." aria-label="بحث">${fixedCat?'':`<select name="cat" aria-label="القسم">${opts(CATS,'','كل الأقسام')}</select>`}<button class="btn">بحث</button></form>
<p style="color:var(--m);margin-bottom:12px">${l.length} إعلان</p>${l.length?`<div class="grid">${l.slice((pg-1)*PER,pg*PER).map(card).join('')}</div>`:`<div class="empty"><p>لا توجد إعلانات مطابقة حاليًا.</p><p style="margin-top:12px"><a class="btn acc" href="/post">كن أول من يضيف إعلانًا</a></p></div>`}${pager(base,keep,pg,pages)}</main>`)}
function home(c){const latest=db.ads.filter(a=>['active','sold'].includes(a.status)).sort((a,b)=>(b.status==='active')-(a.status==='active')||b.created-a.created).slice(0,12);
  html(c,200,{home:1,desc:`${SITE} موقع إعلانات مبوبة مجاني: بيع واشتري سيارات وعقارات وموبايلات وأثاث ووظائف وخدمات في مصر والسعودية والإمارات وباقي الدول العربية.`,path:'/',
  ld:[{'@context':'https://schema.org','@type':'WebSite',name:SITE,url:c.b+'/',inLanguage:'ar',potentialAction:{'@type':'SearchAction',target:c.b+'/ads?q={search_term_string}','query-input':'required name=search_term_string'}},{'@context':'https://schema.org','@type':'Organization',name:SITE,url:c.b+'/'}]},
  `<section class="hero"><div class="w"><h1>بيع واشتري أي حاجة بسهولة</h1><p>إعلانات مجانية — تصفّح، تواصل مع البائع، وأضف إعلانك في دقيقة</p>
<form class="sf" action="/ads"><input name="q" placeholder="ابحث عن سيارة، شقة، موبايل..." aria-label="بحث"><select name="cat" aria-label="القسم">${opts(CATS,'','كل الأقسام')}</select><button class="btn acc">بحث</button></form></div></section>
<main class="w"><h2>تصفح الأقسام</h2><div class="cats">${CATS.map(x=>`<a class="cat" href="/c/${x[0]}"><span>${x[2]}</span>${x[1]}</a>`).join('')}</div>
<h2>أحدث الإعلانات</h2>${latest.length?`<div class="grid">${latest.map(card).join('')}</div>`:`<div class="empty"><p>لا توجد إعلانات بعد.</p><p style="margin-top:12px"><a class="btn acc" href="/post">أضف أول إعلان</a></p></div>`}
<h2>لماذا ${E(SITE)}؟</h2><div class="box"><p>${E(SITE)} منصة عربية للإعلانات المبوبة تجمع البائعين والمشترين في مصر والسعودية والإمارات والكويت وقطر وباقي الدول العربية. أضف إعلانك مجانًا خلال دقيقة مع صور ووصف ورقم تواصل، وتصفح الأقسام: سيارات، عقارات، موبايلات، إلكترونيات، أثاث، وظائف، خدمات والمزيد.</p></div></main>`)}
function adPage(c,a){a.views++;dirty=true;const url=c.b+encodeURI(adUrl(a)),sold=a.status==='sold',mine=c.user&&c.user.id===a.uid,seller=db.users.find(u=>u.id===a.uid);
  const rel=db.ads.filter(x=>x.id!==a.id&&x.status==='active'&&x.cat===a.cat).sort((x,y)=>(y.country===a.country)-(x.country===a.country)||y.created-x.created).slice(0,4);
  const wa=String(a.phone).replace(/\D/g,''),buyBtn=(!sold&&a.price>0&&!mine&&pms(a).length)?`<a class="btn acc" style="width:100%;margin-bottom:10px" href="/buy/${a.id}">🛒 اشتري الآن</a>`:'';
  const chatBtn=mine||sold?'':c.user?`<form method="post" action="/chat/start"><input type="hidden" name="ad" value="${a.id}"><button class="btn" style="width:100%;margin-bottom:10px">💬 راسل البائع</button></form>`:`<a class="btn" style="width:100%;margin-bottom:10px" href="/login?next=${encodeURIComponent(adUrl(a))}">💬 سجّل دخول لمراسلة البائع</a>`,
    payInfo=!sold&&a.price>0&&pms(a).length?`<p style="font-size:13px;color:var(--m);margin-bottom:10px">طرق الدفع: ${pms(a).map(k=>PM[k][1]+' '+PM[k][0]).join(' • ')}</p>`:'';
  const contact=sold?'<p class="tag sold">تم بيع هذا الإعلان</p>':c.user?`<p>📞 <b dir="ltr">${E(a.phone)}</b></p><p style="margin-top:10px"><a class="btn" style="width:100%" href="https://wa.me/${wa}" rel="nofollow noopener" target="_blank">تواصل عبر واتساب</a></p>`:`<p>سجّل الدخول لإظهار رقم التواصل.</p><p style="margin-top:10px"><a class="btn" style="width:100%" href="/login?next=${encodeURIComponent(adUrl(a))}">تسجيل الدخول</a></p>`;
  const ld={'@context':'https://schema.org','@type':'Product',name:a.title,description:a.desc.slice(0,500),category:CM[a.cat][1],url,...(a.images.length?{image:a.images.map(i=>c.b+i)}:{}),...(a.price>0?{offers:{'@type':'Offer',price:a.price,priceCurrency:KM[a.country][3],availability:sold?'https://schema.org/SoldOut':'https://schema.org/InStock',url}}:{})};
  html(c,200,{title:a.title,desc:`${a.title} — ${price(a)} في ${locOf(a)}. ${a.desc}`,path:adUrl(a),type:'product',image:a.images[0]?c.b+a.images[0]:'',noindex:sold,ld:[ld,bcLd(c.b,[['الرئيسية','/'],[CM[a.cat][1],'/c/'+a.cat],[a.title,adUrl(a)]])]},
  `<main class="w"><p class="bc"><a href="/">الرئيسية</a> › <a href="/c/${a.cat}">${E(CM[a.cat][1])}</a> › ${E(a.title)}</p><div class="ad"><article>
${a.images.length?`<div class="gal">${a.images.map(i=>`<img src="${E(i)}" alt="${E(a.title)}" loading="lazy">`).join('')}</div>`:''}
<div class="box"><h1 style="font-size:24px">${E(a.title)}</h1><p class="big">${E(price(a))}</p><p style="color:var(--m);margin:6px 0 14px">📍 ${E(locOf(a))} • ${E(when(a.created))} • ${a.views} مشاهدة • <span class="tag">${E(CM[a.cat][1])}</span></p><h2 style="margin:10px 0 6px;font-size:18px">الوصف</h2><div class="desc">${E(a.desc)}</div></div></article>
<aside><div class="box"><p style="color:var(--m);font-size:14px">المعلن</p>${seller?`<a href="${H(sellerUrl(seller))}" class="sel" style="margin-bottom:4px"><span class="av sm">${E(initial(seller.name))}</span><span><b>${E(seller.name)}</b><br><small style="color:var(--m)">عضو منذ ${new Date(seller.created).getFullYear()} • صفحة البائع ‹</small></span></a>`:'<p><b>مستخدم</b></p>'}<hr style="border:0;border-top:1px solid var(--bd);margin:12px 0">${buyBtn}${payInfo}${chatBtn}${contact}
${mine?`<hr style="border:0;border-top:1px solid var(--bd);margin:12px 0"><form method="post" action="/ad/${a.id}/sold" style="display:inline"><button class="btn sm">تم البيع</button></form> <form method="post" action="/ad/${a.id}/delete" style="display:inline" onsubmit="return confirm('حذف الإعلان نهائيًا؟')"><button class="btn sm red">حذف</button></form>`:''}</div>${bn('side')}</aside></div>
${rel.length?`<h2>إعلانات مشابهة</h2><div class="grid">${rel.map(card).join('')}</div>`:''}</main>`)}
const authForm=(reg,v={},err='',next='/')=>`<main class="w"><form class="f" method="post" action="/${reg?'register':'login'}"><h1 style="font-size:24px">${reg?'إنشاء حساب جديد':'تسجيل الدخول'}</h1>${err?`<p class="er" role="alert">${E(err)}</p>`:''}<input type="hidden" name="next" value="${E(next)}">
${reg?`<label>الاسم<input name="name" required minlength="2" maxlength="40" value="${E(v.name)}" autocomplete="name"></label>
<fieldset class="pmf"><legend>أنا هسجّل كـ</legend><div class="pm"><label><input type="radio" name="role" value="buyer"${v.role==='buyer'?' checked':''}> 🛒 مشتري — أتصفح وأشتري</label><label><input type="radio" name="role" value="seller"${v.role!=='buyer'?' checked':''}> 🏪 بائع — أنشر إعلانات وأبيع</label></div><p style="color:var(--m);font-size:13px">تقدر تغيّر ده في أي وقت من صفحتك.</p></fieldset>
<label>البلد<select name="country" required>${opts(COUNTRIES,v.country||'eg')}</select></label>`:''}<label>البريد الإلكتروني<input type="email" name="email" required maxlength="120" value="${E(v.email)}" autocomplete="email"></label>
<label>كلمة المرور${reg?' (8 أحرف على الأقل)':''}<input type="password" name="password" required minlength="${reg?8:1}" maxlength="100" autocomplete="${reg?'new-password':'current-password'}"></label>${reg?'<label>تأكيد كلمة المرور<input type="password" name="password2" required minlength="8" maxlength="100" autocomplete="new-password"></label>':''}
<button class="btn">${reg?'إنشاء الحساب':'دخول'}</button><p style="text-align:center;font-size:14px">${reg?'لديك حساب؟ <a href="/login" style="color:var(--p)">سجّل دخولك</a>':'ليس لديك حساب؟ <a href="/register" style="color:var(--p)">أنشئ حسابًا مجانيًا</a>'}</p></form></main>`;
const postForm=(v={},err='',needCountry=false)=>`<main class="w"><form class="f" method="post" action="/post" id="pf"><h1 style="font-size:24px">أضف إعلانًا مجانيًا</h1>${err?`<p class="er" role="alert">${E(err)}</p>`:''}
<label>عنوان الإعلان<input name="title" required minlength="5" maxlength="100" value="${E(v.title)}" placeholder="مثال: تويوتا كورولا 2020 بحالة ممتازة"></label>
<div class="row"><label>القسم<select name="cat">${opts(CATS,v.cat)}</select></label>${needCountry?`<label>البلد<select name="country">${opts(COUNTRIES,v.country||'eg')}</select></label>`:`<label>المدينة<input name="city" maxlength="40" value="${E(v.city)}"></label>`}</div>
<div class="row">${needCountry?`<label>المدينة<input name="city" maxlength="40" value="${E(v.city)}"></label>`:''}<label>السعر (اختياري)<input name="price" type="number" min="0" max="1000000000" inputmode="numeric" value="${E(v.price)}"></label></div>
<label>رقم التواصل (واتساب)<input name="phone" type="tel" required maxlength="20" value="${E(v.phone)}" placeholder="+201001234567"></label>
<label>الوصف<textarea name="desc" required minlength="20" maxlength="5000" rows="6">${E(v.desc)}</textarea></label>
<label>الصور (حتى 4، كل صورة حتى 3 ميجا)<input type="file" id="files" accept="image/*" multiple></label><div id="prev"></div><input type="hidden" name="images" id="imgs" value="${E(v.images)}">
${payFields(v)}
<button class="btn" id="sb">نشر الإعلان</button></form></main>
<script>(function(){var f=document.getElementById('files'),pv=document.getElementById('prev'),im=document.getElementById('imgs'),sb=document.getElementById('sb'),urls=im.value?im.value.split(','):[];
function show(u){var i=document.createElement('img');i.src=u;i.alt='';pv.appendChild(i)}urls.forEach(show);
f.onchange=async function(){sb.disabled=true;for(var file of Array.from(f.files)){if(urls.length>=4){alert('الحد الأقصى 4 صور');break}if(file.size>3e6){alert('الصورة أكبر من 3 ميجا');continue}
try{var r=await fetch('/api/upload',{method:'POST',headers:{'Content-Type':file.type},body:file}),d=await r.json();if(d.url){urls.push(d.url);im.value=urls.join(',');show(d.url)}else alert(d.error||'فشل رفع الصورة')}catch(e){alert('فشل رفع الصورة')}}f.value='';sb.disabled=false}})();</script>`;
const buyerGate=()=>`<main class="w"><div class="succ"><div class="ic">🛒</div><h1 style="font-size:22px;margin:8px 0">حسابك حاليًا كمشتري</h1><p style="margin-bottom:14px">علشان تنشر إعلانات لازم تحوّل حسابك لبائع (وتقدر ترجّعه مشتري في أي وقت).</p><form method="post" action="/role"><input type="hidden" name="role" value="seller"><input type="hidden" name="to" value="/post"><button class="btn acc">🏪 حوّل حسابي لبائع</button></form></div></main>`;
function dashboard(c){const u=c.user,seller=roleOf(u)==='seller',mine=db.ads.filter(a=>a.uid===u.id).sort((a,b)=>b.created-a.created),act=mine.filter(a=>a.status==='active').length,sold=mine.filter(a=>a.status==='sold').length,views=mine.reduce((n,a)=>n+a.views,0),
    inc=db.orders.filter(o=>o.sid===u.id),pend=inc.filter(o=>['new','awaiting','paid'].includes(o.status)).length,un=unread(u.id),msg=new URLSearchParams(c.u.searchParams).get('m'),
    sw=`<form method="post" action="/role" style="display:inline"><input type="hidden" name="role" value="${seller?'buyer':'seller'}"><button class="btn sm${seller?'':' acc'}">${seller?'🛒 حوّل حسابي لمشتري':'🏪 حوّل حسابي لبائع'}</button></form>`,
    cn=KM[u.country]?KM[u.country][1]:'';
  const head=`<div class="box sel" style="margin:20px 0"><span class="av">${E(initial(u.name))}</span><div style="flex:1;min-width:200px"><h1 style="font-size:24px">${E(u.name)}</h1><div class="sst"><span>${seller?'🏪 بائع':'🛒 مشتري'}</span>${cn?`<span>🌍 ${E(cn)}</span>`:''}<span dir="ltr">${E(u.email)}</span></div></div><div style="display:flex;gap:8px;flex-wrap:wrap">${sw}${seller?`<a class="btn sm" href="/profile">✏️ تعديل صفحتي</a>`:''}</div></div>${msg?`<p class="ok" style="margin-bottom:12px">${E(msg)}</p>`:''}`;
  if(!seller)return html(c,200,{title:'حسابي',noindex:1,path:'/dashboard'},`<main class="w">${head}<div class="stat"><div>${db.orders.filter(o=>o.uid===u.id).length}<small>مشترياتي</small></div><div>${un}<small>رسائل غير مقروءة</small></div></div><p style="margin:18px 0;display:flex;gap:8px;flex-wrap:wrap"><a class="btn" href="/ads">تصفّح الإعلانات</a><a class="btn acc" href="/my-orders">طلباتي</a><a class="btn acc" href="/chats">💬 رسائلي</a></p><div class="empty"><p>عايز تبيع حاجة؟ حوّل حسابك لبائع وانشر إعلانك.</p></div></main>`);
  html(c,200,{title:'لوحة البائع',noindex:1,path:'/dashboard'},`<main class="w">${head}<div class="stat"><div>${act}<small>إعلان منشور</small></div><div>${sold}<small>تم بيعه</small></div><div>${views}<small>مشاهدة</small></div><div>${pend}<small>طلب مفتوح</small></div><div>${un}<small>رسالة جديدة</small></div></div>
<p style="margin:18px 0;display:flex;gap:8px;flex-wrap:wrap"><a class="btn acc" href="/post">+ أضف إعلان</a><a class="btn" href="/my-orders">📦 الطلبات${pend?` (${pend})`:''}</a><a class="btn" href="/chats">💬 الرسائل${un?` (${un})`:''}</a><a class="btn" href="${H(sellerUrl(u))}">صفحتي العامة</a></p>
<h2>إعلاناتي (${mine.length})</h2>${mine.length?`<div class="tbw"><table><tr><th></th><th>الإعلان</th><th>السعر</th><th>الحالة</th><th>مشاهدات</th><th>التاريخ</th><th></th></tr>${mine.map(a=>`<tr><td>${a.images[0]?`<img class="thumb" src="${E(a.images[0])}" alt="" loading="lazy">`:CM[a.cat][2]}</td><td><a href="${H(adUrl(a))}"><b>${E(a.title)}</b></a><br><small>${E(CM[a.cat][1])} • ${E(locOf(a))}</small></td><td>${E(price(a))}</td><td>${badge(AST,a.status)}</td><td>${a.views}</td><td>${E(when(a.created))}</td><td style="white-space:nowrap">${a.status==='hidden'?'<small>مخفي من الإدارة</small> ':`<form method="post" action="/ad/${a.id}/sold" style="display:inline"><button class="btn sm">${a.status==='sold'?'إعادة نشر':'تم البيع'}</button></form> `}<form method="post" action="/ad/${a.id}/delete" style="display:inline" onsubmit="return confirm('حذف الإعلان نهائيًا؟')"><button class="btn sm red">حذف</button></form></td></tr>`).join('')}</table></div>`:`<div class="empty"><p>لم تضف أي إعلان بعد.</p><p style="margin-top:12px"><a class="btn acc" href="/post">أضف إعلانك الأول</a></p></div>`}</main>`)}
function adminPage(c){const sp=c.u.searchParams,TB=['ads','users','banners'],tab=TB.includes(sp.get('tab'))?sp.get('tab'):'ads',msg=sp.get('m'),q=(sp.get('q')||'').trim().slice(0,60),stf=AST[sp.get('s')]?sp.get('s'):'',un=id=>db.users.find(u=>u.id===id)||{};
  const tb=(h,rows,none)=>rows.length?`<div class="tbw"><table><tr>${h.map(x=>`<th>${x}</th>`).join('')}</tr>${rows.join('')}</table></div>`:`<div class="empty"><p>${none}</p></div>`,fm=(act,t,cls='',cf='')=>`<form method="post" action="${act}" style="display:inline"${cf?` onsubmit="return confirm('${cf}')"`:''}><button class="btn sm${cls}">${t}</button></form> `;
  let body='',pgr='';
  if(tab==='ads'){const l=db.ads.filter(a=>(!stf||a.status===stf)&&(!q||norm(a.title+' '+a.phone+' '+(un(a.uid).email||'')+' '+(un(a.uid).name||'')).includes(norm(q)))).sort((a,b)=>b.created-a.created),pages=Math.max(1,Math.ceil(l.length/30)),pg=Math.min(Math.max(1,parseInt(sp.get('page'))||1),pages),keep=new URLSearchParams({tab:'ads'});if(q)keep.set('q',q);if(stf)keep.set('s',stf);
    body=`<form class="sf" action="/admin" style="max-width:none;margin:0 0 14px"><input type="hidden" name="tab" value="ads"><input name="q" value="${E(q)}" placeholder="ابحث بالعنوان أو اسم/بريد/رقم المعلن"><select name="s" aria-label="الحالة">${opts(Object.keys(AST).map(k=>[k,AST[k][0]]),stf,'كل الحالات')}</select><button class="btn">بحث</button></form><p style="color:var(--m);margin-bottom:10px">${l.length} إعلان</p>`
    +tb(['','الإعلان','السعر','المعلن','الحالة','مشاهدات','التاريخ',''],l.slice((pg-1)*30,pg*30).map(a=>{const u=un(a.uid);return`<tr><td>${a.images[0]?`<img class="thumb" src="${E(a.images[0])}" alt="" loading="lazy">`:CM[a.cat][2]}</td><td><a href="${H(adUrl(a))}"><b>${E(a.title)}</b></a><br><small>${E(CM[a.cat][1])} • ${E(locOf(a))}</small></td><td>${E(price(a))}</td><td>${E(u.name||'-')}<br><small dir="ltr">${E(u.email||'')}</small><br><small dir="ltr">${E(a.phone)}</small></td><td>${badge(AST,a.status)}</td><td>${a.views}</td><td>${E(when(a.created))}</td><td style="white-space:nowrap">${fm(`/admin/ad/${a.id}/toggle`,a.status==='hidden'?'إظهار':'إخفاء')}${fm(`/admin/ad/${a.id}/delete`,'حذف',' red','حذف الإعلان؟')}</td></tr>`}),'لا توجد إعلانات مطابقة');pgr=pager('/admin',keep,pg,pages)}
  else if(tab==='users')body=tb(['الاسم','البريد','النوع','البلد','التسجيل','الإعلانات',''],[...db.users].sort((a,b)=>b.id-a.id).slice(0,200).map(u=>`<tr><td>${E(u.name)}</td><td dir="ltr">${E(u.email)}</td><td>${roleOf(u)==='seller'?'بائع':'مشتري'}</td><td>${E(KM[u.country]?KM[u.country][1]:'-')}</td><td>${E(when(u.created))}</td><td>${db.ads.filter(a=>a.uid===u.id).length}</td><td>${fm(`/admin/user/${u.id}/delete`,'حذف',' red','حذف المستخدم وكل إعلاناته؟')}</td></tr>`),'لا يوجد مستخدمون');
  else if(tab==='banners')body=`<form class="f" method="post" action="/admin/banner/add" style="margin:0 0 18px;max-width:none"><h2 style="margin:0;font-size:18px">إضافة مساحة إعلانية</h2><p style="color:var(--m);font-size:13.5px">المقاسات المناسبة: الشريط العلوي 728×90 — جانب الإعلان 300×250. المساحة صغيرة عمدًا عشان ما تضايقش الزائر، ولو ما فيش إعلان شغال المساحة بتختفي.</p>
<div class="row"><label>المكان<select name="slot"><option value="top">شريط علوي (تحت الهيدر)</option><option value="side">جانب صفحة الإعلان</option></select></label><label>رابط الإعلان عند الضغط (اختياري)<input name="link" dir="ltr" placeholder="https://..."></label></div>
<label>صورة الإعلان<input type="file" id="bf" accept="image/*"></label><div id="bprev"></div><input type="hidden" name="img" id="bimg">
<label>أو كود إعلان جاهز (مثل جوجل أدسنس) — لو كتبته هيتجاهل الصورة<textarea name="code" rows="3" dir="ltr" maxlength="5000"></textarea></label><button class="btn">إضافة</button></form>
<script>(function(){var f=document.getElementById('bf'),h=document.getElementById('bimg'),pv=document.getElementById('bprev');f.onchange=async function(){var file=f.files[0];if(!file)return;if(file.size>3e6){alert('الصورة أكبر من 3 ميجا');return}try{var r=await fetch('/api/admin-upload',{method:'POST',headers:{'Content-Type':file.type},body:file}),d=await r.json();if(d.url){h.value=d.url;pv.innerHTML='<img src="'+d.url+'" alt="" style="max-height:90px;border-radius:8px">'}else alert(d.error||'فشل الرفع')}catch(e){alert('فشل الرفع')}}})();</script>`
    +tb(['','المكان','الرابط','الحالة',''],db.banners.map(b=>`<tr><td>${b.code?'كود':`<img class="thumb" style="width:auto;max-width:140px;height:auto;max-height:44px" src="${E(b.img)}" alt="">`}</td><td>${b.slot==='top'?'شريط علوي':'جانب الإعلان'}</td><td dir="ltr">${E(b.link||'-')}</td><td>${b.on?'<span class="st g">شغال</span>':'<span class="st r">موقوف</span>'}</td><td style="white-space:nowrap">${fm(`/admin/banner/${b.id}/toggle`,b.on?'إيقاف':'تشغيل')}${fm(`/admin/banner/${b.id}/delete`,'حذف',' red','حذف المساحة الإعلانية؟')}</td></tr>`),'لا توجد مساحات إعلانية بعد');
;
  html(c,200,{title:'لوحة التحكم',noindex:1,path:'/admin'},`<main class="w"><h1 style="font-size:26px;margin:20px 0">لوحة التحكم <form method="post" action="/admin/logout" style="display:inline"><button class="btn sm red">خروج</button></form></h1>${msg?`<p class="ok">${E(msg)}</p>`:''}
<div class="stat"><div>${db.users.length}<small>مستخدم</small></div><div>${db.ads.length}<small>إعلان</small></div><div>${db.ads.reduce((n,a)=>n+a.views,0)}<small>مشاهدة</small></div></div>
<p style="margin:18px 0;display:flex;gap:8px;flex-wrap:wrap">${[['ads','الإعلانات'],['users','المستخدمون'],['banners','المساحات الإعلانية']].map(([k,t])=>`<a class="btn${tab===k?'':' acc'} sm" href="/admin?tab=${k}">${t}</a>`).join('')}</p>${body}${pgr}</main>`)}
const adminLogin=(c,err)=>html(c,err?401:200,{title:'دخول الأدمن',noindex:1,path:'/admin'},`<main class="w"><form class="f" method="post" action="/admin/login"><h1 style="font-size:24px">دخول الأدمن</h1>${err?`<p class="er">${E(err)}</p>`:''}<label>اسم المستخدم<input name="user" required autocomplete="username"></label><label>كلمة المرور<input type="password" name="password" required autocomplete="current-password"></label><button class="btn">دخول</button></form></main>`);

// ---------- الدفع والطلبات والمساحات الإعلانية ----------
const DEP=0.2,PM={vodafone:['ادفع عربون','💰'],instapay:['إنستا باي','🏦'],cod:['الدفع عند الاستلام','💵']};
const payOf=a=>{if(a.pay)return{vodafone:a.pay.vodafone||'',instapay:a.pay.instapay||'',cod:!!a.pay.cod};const u=db.users.find(x=>x.id===a.uid)||{};if(u.vodafone!==undefined||u.instapay!==undefined||u.cod!==undefined)return{vodafone:u.vodafone||'',instapay:u.instapay||'',cod:!!u.cod};const t=db.settings||{};return{vodafone:t.vodafone||'',instapay:t.instapay||'',cod:t.cod!==false}};
const pms=a=>{const t=payOf(a);return['vodafone','instapay','cod'].filter(k=>!!t[k])};
const OST={new:['جديد','y'],awaiting:['بانتظار تأكيد الدفع','y'],paid:['تم الدفع','g'],done:['تم التسليم','g'],cancelled:['ملغي','r']},AST={active:['منشور','g'],sold:['تم البيع','y'],hidden:['مخفي','r']};
const badge=(m,k)=>`<span class="st ${m[k][1]}">${m[k][0]}</span>`,cur=o=>`${nf.format(o.amt)} ${o.cur}`;
const buyable=(c,a)=>!!a&&a.status==='active'&&a.price>0&&a.uid!==c.user.id&&pms(a).length>0;
const bn=slot=>{const l=db.banners.filter(b=>b.on&&b.slot===slot);if(!l.length)return'';const b=l[Math.floor(Math.random()*l.length)],inner=(b.code||`<img src="${E(b.img)}" alt="إعلان" loading="lazy">`)+'<small>إعلان</small>',
  el=!b.code&&b.link?`<a class="adv ${slot}" href="${E(b.link)}" target="_blank" rel="sponsored nofollow noopener">${inner}</a>`:`<div class="adv ${slot}">${inner}</div>`;return slot==='top'?`<div class="w">${el}</div>`:el};
const myOrderOf=(c,id)=>{const o=db.orders.find(x=>x.id===+id);return o&&c.user&&(o.uid===c.user.id||o.sid===c.user.id)?o:null};
const buyForm=(a,v={},err='')=>`<main class="w"><form class="f" method="post" action="/buy/${a.id}"><h1 style="font-size:24px">إتمام الشراء</h1><div class="box"><b>${E(a.title)}</b><p class="big">${E(price(a))}</p></div>${err?`<p class="er" role="alert">${E(err)}</p>`:''}
<label>الاسم<input name="name" required minlength="2" maxlength="60" value="${E(v.name)}"></label><label>رقم الموبايل<input name="phone" type="tel" required maxlength="20" value="${E(v.phone)}"></label><label>عنوان التسليم بالتفصيل<textarea name="addr" required minlength="10" maxlength="300" rows="3">${E(v.addr)}</textarea></label>
<div class="pm"><b>طريقة الدفع</b>${pms(a).map((k,i)=>`<label><input type="radio" name="method" value="${k}"${(v.method?v.method===k:i===0)?' checked':''}> ${PM[k][1]} ${PM[k][0]}</label>`).join('')}</div><button class="btn acc">تأكيد الطلب</button></form></main>`;
const payPage=(o,err='')=>{const vf=o.method==='vodafone',num=o.to||(vf?db.settings.vodafone:db.settings.instapay);return`<main class="w"><form class="f" method="post" action="/order/${o.id}/pay"><h1 style="font-size:24px">${PM[o.method][1]} الدفع عبر ${PM[o.method][0]}</h1>${err?`<p class="er" role="alert">${E(err)}</p>`:''}
<div class="box"><p style="color:var(--m)">${vf?'العربون المطلوب (20% من السعر)':'المبلغ المطلوب'}</p><p class="big">${E(vf&&o.dep?`${nf.format(o.dep)} ${o.cur}`:cur(o))}</p>${vf&&o.dep?`<p style="color:var(--m);font-size:14px">السعر الكلي ${E(cur(o))} — والباقي (${E(nf.format(o.amt-o.dep)+' '+o.cur)}) تدفعه للبائع عند الاستلام.</p>`:''}<hr style="border:0;border-top:1px solid var(--bd);margin:12px 0"><p style="color:var(--m)">${vf?'حوّل العربون على رقم فودافون كاش':'حوّل على عنوان إنستا باي'}${o.sname?' للبائع '+E(o.sname):''}</p><p class="big" dir="ltr" style="user-select:all">${E(num||'')}</p></div>
<ol style="padding-right:20px"><li>حوّل ${vf?'مبلغ العربون':'المبلغ'} بالظبط على الرقم أعلاه.</li><li>انسخ رقم العملية (أو اكتب الرقم/الحساب اللي حوّلت منه).</li><li>اكتبه تحت واضغط «تأكيد الدفع».</li></ol>
<label>رقم العملية أو رقم المحفظة/الحساب اللي حوّلت منه<input name="ref" required minlength="4" maxlength="60" dir="ltr"></label><button class="btn acc">تأكيد الدفع</button></form></main>`};
const orderPage=(c,o)=>{const buyer=o.uid===c.user.id,a=db.ads.find(x=>x.id===o.adId),box=(ic,t,d)=>`<div class="succ"><div class="ic">${ic}</div><h1 style="font-size:24px;margin:8px 0">${t}</h1><p>${d}</p></div>`;let top='';
  if(buyer){if(o.status==='cancelled')top=box('❌','تم إلغاء الطلب','لو كنت حوّلت فلوس، راسل البائع.');
    else if(o.method==='cod')top=o.status==='done'?box('✅','تم تسليم طلبك','شكرًا لتعاملك معنا.'):box('✅','تم تسجيل طلبك',`هتدفع ${E(cur(o))} عند الاستلام، والبائع هيتواصل معاك لتأكيد الموعد.`);
    else if(o.status==='paid'||o.status==='done')top=box('✅',o.method==='vodafone'?'تم استلام العربون':'تم الدفع بنجاح',o.method==='vodafone'?'البائع أكد استلام العربون، وهيتواصل معاك لتسليم الطلب وتسوية الباقي.':'تم تأكيد دفعك، والبائع هيتواصل معاك لتسليم الطلب.');
    else if(o.status==='awaiting')top=box('⏳','استلمنا بيانات الدفع','البائع بيراجع التحويل، وأول ما يأكده هتظهر هنا رسالة «تم الدفع بنجاح». الصفحة بتتحدّث لوحدها.')+'<script>setTimeout(function(){location.reload()},20000)</script>';
    else top=box('💳','الطلب لسه ما اتدفعش',`<a class="btn acc" href="/order/${o.id}/pay">كمّل الدفع</a>`)}
  const cform=buyer?`<form method="post" action="/chat/start" style="display:inline"><input type="hidden" name="${a?'ad':'seller'}" value="${a?a.id:o.sid}"><button class="btn sm">💬 راسل البائع</button></form> ${o.status==='new'?fmx(`/order/${o.id}/cancel`,'إلغاء الطلب',' red','إلغاء الطلب؟'):''}`:(a?`<form method="post" action="/chat/start" style="display:inline"><input type="hidden" name="ad" value="${a.id}"><input type="hidden" name="with" value="${o.uid}"><button class="btn sm">💬 راسل المشتري</button></form> `:'')+ordActs(o);
  return`<main class="w">${top}<div class="box" style="max-width:640px;margin:18px auto"><h2 style="margin:0 0 8px;font-size:18px">تفاصيل الطلب رقم ${o.id}</h2><p>${a?`<a href="${H(adUrl(a))}" style="color:var(--p)"><b>${E(o.title)}</b></a>`:`<b>${E(o.title)}</b>`}</p><p>المبلغ: <b>${E(cur(o))}</b> • ${PM[o.method][1]} ${E(PM[o.method][0])} • ${badge(OST,o.status)}</p>${buyer?'':`<hr style="border:0;border-top:1px solid var(--bd);margin:10px 0"><p>المشتري: <b>${E(o.name)}</b> — <span dir="ltr">${E(o.phone)}</span></p><p>العنوان: ${E(o.addr)}</p>`}<p style="margin-top:14px">${cform}<a class="btn sm" href="${a?H(adUrl(a)):'/'}">الرجوع للإعلان</a> <a class="btn sm acc" href="/my-orders">كل طلباتي</a></p></div></main>`};
function myOrders(c){const mine=db.orders.filter(o=>o.uid===c.user.id).sort((a,b)=>b.created-a.created),inc=db.orders.filter(o=>o.sid===c.user.id).sort((a,b)=>b.created-a.created);
  const tb=(l,sel)=>l.length?`<div class="tbw"><table><tr><th>#</th><th>الإعلان</th><th>المبلغ</th><th>الدفع</th><th>الحالة</th>${sel?'<th>المشتري</th>':''}<th></th></tr>${l.map(o=>`<tr><td>${o.id}</td><td>${E(o.title)}</td><td>${E(cur(o))}</td><td>${E(PM[o.method][0])}</td><td>${badge(OST,o.status)}</td>${sel?`<td>${E(o.name)}<br><small dir="ltr">${E(o.phone)}</small><br><small>${E(o.addr)}</small></td>`:''}<td style="white-space:nowrap"><a class="btn sm" href="/order/${o.id}">عرض</a> ${sel?ordActs(o):''}</td></tr>`).join('')}</table></div>`:'<div class="empty"><p>لا توجد طلبات.</p></div>';
  html(c,200,{title:'طلباتي',noindex:1,path:'/my-orders'},`<main class="w"><h1 style="font-size:26px;margin:20px 0">مشترياتي (${mine.length})</h1>${tb(mine,0)}<h2>طلبات على إعلاناتي (${inc.length})</h2>${tb(inc,1)}</main>`)}

// ---------- صفحة البائع + بيانات الدفع + الشات ----------
const payFields=v=>`<fieldset class="pmf"><legend>💰 استلام العربون والدفع (اختياري)</legend><p style="color:var(--m);font-size:13.5px">لو عايز المشتري يدفع من خلال الموقع اكتب بياناتك. بتظهر للمشتري وقت الشراء بس، وبتتحفظ في صفحتك للإعلانات الجاية. لو سبتها فاضية هيتواصل معاك بالشات أو الواتساب.</p>
<div class="row"><label>رقم استلام العربون (فودافون كاش)<input name="vodafone" dir="ltr" inputmode="tel" maxlength="20" value="${E(v.vodafone)}" placeholder="01012345678"></label><label>عنوان إنستا باي<input name="instapay" dir="ltr" maxlength="60" value="${E(v.instapay)}" placeholder="name@instapay"></label></div>
<label style="display:flex;align-items:center;gap:8px;font-weight:600"><input type="checkbox" name="cod" value="1" style="width:auto"${v.cod?' checked':''}> الدفع عند الاستلام</label></fieldset>`;
const cleanPay=b=>({vodafone:String(b.vodafone||'').replace(/[^\d+]/g,''),instapay:String(b.instapay||'').trim().replace(/[\u0000-\u001f<>"']/g,''),cod:b.cod==='1'});
const payErr=p=>p.vodafone&&(p.vodafone.length<10||p.vodafone.length>15)?'رقم فودافون كاش غير صحيح':p.instapay.length>60?'عنوان إنستا باي طويل':'';
const profileForm=(u,err='')=>`<main class="w"><form class="f" method="post" action="/profile"><h1 style="font-size:24px">صفحتي كبائع</h1>${err?`<p class="er" role="alert">${E(err)}</p>`:''}
<p style="color:var(--m);font-size:14px">دي بياناتك اللي بتظهر للمشترين في صفحتك العامة. <a href="${H(sellerUrl(u))}" style="color:var(--p)">عرض صفحتي ‹</a></p>
<div class="row"><label>الاسم<input name="name" required minlength="2" maxlength="40" value="${E(u.name)}"></label><label>البلد<select name="country">${opts(COUNTRIES,u.country||'eg')}</select></label></div><label>المدينة<input name="city" maxlength="40" value="${E(u.city)}"></label>
<label>نبذة عنك أو عن متجرك<textarea name="bio" rows="3" maxlength="300">${E(u.bio)}</textarea></label>${payFields({vodafone:u.vodafone,instapay:u.instapay,cod:u.cod!==false})}<button class="btn">حفظ</button></form></main>`;
function sellerPage(c,s){
  const sp=c.u.searchParams,all=db.ads.filter(a=>a.uid===s.id&&['active','sold'].includes(a.status)).sort((a,b)=>(b.status==='active')-(a.status==='active')||b.created-a.created),sold=db.ads.filter(a=>a.uid===s.id&&a.status==='sold').length,
    pages=Math.max(1,Math.ceil(all.length/PER)),pg=Math.min(Math.max(1,parseInt(sp.get('page'))||1),pages),base=sellerUrl(s),me=c.user&&c.user.id===s.id,
    pm=['vodafone','instapay','cod'].filter(k=>k==='cod'?s.cod===true:!!s[k]),
    act=me?`<a class="btn" href="/profile">✏️ تعديل صفحتي</a>`:c.user?`<form method="post" action="/chat/start"><input type="hidden" name="seller" value="${s.id}"><button class="btn">💬 راسل البائع</button></form>`:`<a class="btn" href="/login?next=${encodeURIComponent(base)}">💬 سجّل دخول لمراسلة البائع</a>`;
  html(c,200,{title:`${s.name} — صفحة البائع`,desc:`تصفح ${all.length} إعلان للبائع ${s.name} على ${SITE}. ${s.bio||'تواصل مباشر مع البائع.'}`,path:base+(pg>1?`?page=${pg}`:''),noindex:!all.length,ld:[bcLd(c.b,[['الرئيسية','/'],[s.name,base]])]},
`<main class="w"><p class="bc"><a href="/">الرئيسية</a> › البائع</p>
<div class="box sel"><span class="av">${E(initial(s.name))}</span><div style="flex:1;min-width:200px"><h1 style="font-size:24px">${E(s.name)}</h1>
<div class="sst">${s.city?`<span>📍 ${E(s.city)}</span>`:''}<span>عضو منذ <b>${new Date(s.created).getFullYear()}</b></span><span><b>${all.length-sold}</b> إعلان منشور</span><span><b>${sold}</b> تم بيعه</span></div>
${s.bio?`<p class="desc" style="margin-top:8px">${E(s.bio)}</p>`:''}${pm.length?`<p style="margin-top:8px;font-size:14px">طرق الدفع: ${pm.map(k=>`<span class="pmb">${PM[k][1]} ${E(PM[k][0])}</span>`).join('')}</p>`:''}</div><div>${act}</div></div>
<h2>إعلانات البائع <small>(${all.length})</small></h2>${all.length?`<div class="grid">${all.slice((pg-1)*PER,pg*PER).map(card).join('')}</div>${pager(base,{},pg,pages)}`:'<div class="empty"><p>لا توجد إعلانات منشورة حاليًا.</p></div>'}</main>`)}

// --- الطلبات: البائع هو اللي بيأكد الدفع والتسليم ---
const fmx=(act,t,cls='',cf='')=>`<form method="post" action="${act}" style="display:inline"${cf?` onsubmit="return confirm('${cf}')"`:''}><button class="btn sm${cls}">${t}</button></form> `;
const ordActs=o=>{const open=['new','awaiting','paid'].includes(o.status);return(o.method!=='cod'&&['new','awaiting'].includes(o.status)?fmx(`/order/${o.id}/paid`,'تأكيد استلام الدفع'):'')+(open?fmx(`/order/${o.id}/done`,'تم التسليم'):'')+(open?fmx(`/order/${o.id}/cancel`,'إلغاء',' red','إلغاء الطلب؟'):'')};
function orderAct(o,act){
  if(act==='paid'&&o.method!=='cod'&&['new','awaiting'].includes(o.status)){o.status='paid';return}
  if(act==='done'&&['new','awaiting','paid'].includes(o.status)){o.status='done';const a=db.ads.find(x=>x.id===o.adId);if(a&&a.status==='active')a.status='sold';return}
  if(act==='cancel'&&['new','awaiting','paid'].includes(o.status))o.status='cancelled'}

// --- الشات ---
const myConv=(u,id)=>q1('SELECT * FROM convs WHERE id=? AND (buyer_id=? OR seller_id=?)',+id,u.id,u.id);
function sendMsg(u,cv,body){
  const t=String(body||'').replace(/\r\n/g,'\n').trim();
  if(!t)return[400,'اكتب رسالة أولًا'];if(t.length>1000)return[400,'الرسالة طويلة (الحد الأقصى 1000 حرف)'];
  if(rate('m'+u.id,30,6e4))return[429,'بتبعت بسرعة، استنى شوية وجرّب تاني'];
  const n=Date.now();qr('INSERT INTO msgs(conv_id,sender_id,body,created) VALUES(?,?,?,?)',cv.id,u.id,t,n);qr('UPDATE convs SET last_at=? WHERE id=?',n,cv.id);return null}
function newMsgs(u,cv,after){
  const l=qa('SELECT id,sender_id,body,created FROM msgs WHERE conv_id=? AND id>? ORDER BY id LIMIT 200',cv.id,after);
  qr('UPDATE msgs SET read_at=? WHERE conv_id=? AND sender_id!=? AND read_at IS NULL',Date.now(),cv.id,u.id);
  return l.map(m=>({id:m.id,mine:m.sender_id===u.id,body:m.body,t:m.created}))}
function startChat(c,b){
  const me=c.user.id,ad=+b.ad?db.ads.find(x=>x.id===+b.ad):null;let adId=0,buyer=me,seller=+b.seller;
  if(+b.ad&&!ad)return null;
  if(ad){adId=ad.id;seller=ad.uid;if(ad.uid===me&&+b.with)buyer=+b.with}
  if(buyer===seller||!db.users.some(x=>x.id===seller)||!db.users.some(x=>x.id===buyer))return null;
  let cv=q1('SELECT id FROM convs WHERE ad_id=? AND buyer_id=? AND seller_id=?',adId,buyer,seller);
  if(!cv){if(rate('cs'+me,20,36e5))return 0;const n=Date.now();qr('INSERT INTO convs(ad_id,buyer_id,seller_id,created,last_at) VALUES(?,?,?,?,?)',adId,buyer,seller,n,n);cv=q1('SELECT id FROM convs WHERE ad_id=? AND buyer_id=? AND seller_id=?',adId,buyer,seller)}
  return cv.id}
function chatsPage(c){
  const me=c.user.id,l=qa(`SELECT c.*,(SELECT body FROM msgs WHERE conv_id=c.id ORDER BY id DESC LIMIT 1) lb,(SELECT COUNT(*) FROM msgs WHERE conv_id=c.id AND sender_id!=? AND read_at IS NULL) un FROM convs c WHERE (c.buyer_id=? OR c.seller_id=?) AND EXISTS(SELECT 1 FROM msgs WHERE conv_id=c.id) ORDER BY c.last_at DESC LIMIT 100`,me,me,me);
  html(c,200,{title:'رسائلي',noindex:1,path:'/chats'},`<main class="w" style="max-width:760px"><h1 style="font-size:26px;margin:20px 0">💬 رسائلي</h1>${l.length?`<div class="cl">${l.map(x=>{const oid=x.buyer_id===me?x.seller_id:x.buyer_id,o=db.users.find(u=>u.id===oid),a=x.ad_id?db.ads.find(y=>y.id===x.ad_id):null;
    return`<a href="/chat/${x.id}"><span class="av sm">${E(initial(o&&o.name))}</span><span class="tx"><b>${E(o?o.name:'مستخدم')}</b>${a?` <small style="color:var(--m)">• ${E(a.title)}</small>`:''}<p>${E(x.lb)}</p></span>${x.un?`<span class="bdg">${x.un}</span>`:''}<small style="color:var(--m)">${E(when(x.last_at))}</small></a>`}).join('')}</div>`:'<div class="empty"><p>لا توجد رسائل بعد.</p><p style="margin-top:8px">افتح أي إعلان واضغط «راسل البائع» لتبدأ محادثة.</p></div>'}</main>`)}
function chatPage(c,cv){
  const me=c.user.id,oid=cv.buyer_id===me?cv.seller_id:cv.buyer_id,o=db.users.find(x=>x.id===oid),a=cv.ad_id?db.ads.find(x=>x.id===cv.ad_id):null,
    ms=qa('SELECT * FROM (SELECT id,sender_id,body,created FROM msgs WHERE conv_id=? ORDER BY id DESC LIMIT 100) ORDER BY id',cv.id);
  qr('UPDATE msgs SET read_at=? WHERE conv_id=? AND sender_id!=? AND read_at IS NULL',Date.now(),cv.id,me);
  const last=ms.length?ms[ms.length-1].id:0,msg=m=>`<div class="msg ${m.sender_id===me?'me':'them'}" data-id="${m.id}"><p>${E(m.body)}</p><small data-t="${m.created}"></small></div>`;
  html(c,200,{title:`محادثة مع ${o?o.name:'مستخدم'}`,noindex:1,path:'/chat/'+cv.id},
`<main class="w"><div class="chat"><div class="chh"><span class="av sm">${E(initial(o&&o.name))}</span><div>${o?`<a href="${H(sellerUrl(o))}"><b>${E(o.name)}</b></a>`:'<b>مستخدم</b>'}<br><small><a href="/chats" style="color:var(--m)">‹ كل المحادثات</a></small></div>${a?`<a class="ctx" href="${H(adUrl(a))}">بخصوص: ${E(a.title)}<br><b>${E(price(a))}</b></a>`:''}</div>
<div class="msgs" id="msgs" data-last="${last}">${ms.length?ms.map(msg).join(''):'<p id="none" style="text-align:center;color:var(--m);margin:auto">ابدأ المحادثة بكتابة رسالتك 👇</p>'}</div>
<form class="cf" id="cf" method="post" action="/chat/${cv.id}"><textarea name="body" rows="2" maxlength="1000" required placeholder="اكتب رسالتك..."></textarea><button class="btn">إرسال</button></form></div>
<p class="warn">🔒 للأمان: ما تبعتش فلوس قبل ما تتأكد من البائع والمنتج، وما تشاركش أكواد التفعيل أو بيانات بطاقتك البنكية مع أي حد.</p></main>
<script>(function(){var box=document.getElementById('msgs'),f=document.getElementById('cf'),ta=f.querySelector('textarea'),bt=f.querySelector('button'),last=+box.getAttribute('data-last')||0,id=${cv.id};
function tm(el){var t=+el.getAttribute('data-t');if(t)el.textContent=new Date(t).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})}
function down(){box.scrollTop=box.scrollHeight}
function add(m){if(box.querySelector('[data-id="'+m.id+'"]'))return;var n=document.getElementById('none');if(n)n.remove();var d=document.createElement('div');d.className='msg '+(m.mine?'me':'them');d.setAttribute('data-id',m.id);var p=document.createElement('p');p.textContent=m.body;var s=document.createElement('small');s.setAttribute('data-t',m.t);tm(s);d.appendChild(p);d.appendChild(s);box.appendChild(d);last=Math.max(last,m.id)}
box.querySelectorAll('small[data-t]').forEach(tm);down();
async function poll(){try{var r=await fetch('/api/chat/'+id+'/msgs?after='+last,{credentials:'same-origin',cache:'no-store'});if(!r.ok)return;var d=await r.json();if(d.msgs.length){var near=box.scrollHeight-box.scrollTop-box.clientHeight<140;d.msgs.forEach(add);if(near)down()}}catch(e){}}
setInterval(function(){if(!document.hidden)poll()},3500);
f.addEventListener('submit',async function(e){e.preventDefault();var t=ta.value.trim();if(!t)return;bt.disabled=true;try{var r=await fetch('/api/chat/'+id+'/send',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({body:t})}),d=await r.json();if(d.error)alert(d.error);else{ta.value='';await poll();down()}}catch(x){alert('فشل الإرسال، حاول تاني')}bt.disabled=false;ta.focus()});
ta.addEventListener('keydown',function(e){if(e.key==='Enter'&&!e.shiftKey&&!e.isComposing){e.preventDefault();f.requestSubmit()}});
})()</script>`)}
// ---------- الخادم ----------
http.createServer(async(req,res)=>{try{
  const u=new URL(req.url,'http://x');let p;try{p=decodeURIComponent(u.pathname)}catch{p=u.pathname}p=p.replace(/\/+$/,'')||'/';
  const m=req.method==='HEAD'?'GET':req.method,c={req,res,u,p,user:getUser(req),b:process.env.SITE_URL||`${req.headers['x-forwarded-proto']||'http'}://${req.headers.host}`},ip=req.socket.remoteAddress;
  res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('X-Frame-Options','SAMEORIGIN');res.setHeader('Referrer-Policy','strict-origin-when-cross-origin');if(https(req))res.setHeader('Strict-Transport-Security','max-age=31536000');
  if(m==='POST'&&!okOrigin(req)){res.writeHead(403);return res.end('Forbidden')}
  let r;
  if(m==='GET'){
    if(p==='/style.css')return out(c,200,CSS,'text/css; charset=utf-8',{'Cache-Control':'public,max-age=86400'});
    if(p==='/favicon.svg')return out(c,200,LOGO.replace('<svg ','<svg xmlns="http://www.w3.org/2000/svg" '),'image/svg+xml',{'Cache-Control':'public,max-age=86400'});
    if(p==='/robots.txt')return out(c,200,`User-agent: *\nAllow: /\nDisallow: /admin\nDisallow: /post\nDisallow: /my-ads\nDisallow: /dashboard\nDisallow: /api/\nDisallow: /buy\nDisallow: /order\nDisallow: /my-orders\nDisallow: /chat\nDisallow: /chats\nDisallow: /profile\nSitemap: ${c.b}/sitemap.xml\n`,'text/plain; charset=utf-8');
    if(p==='/sitemap.xml'){const x=s=>s.replace(/&/g,'&amp;'),us=[['/',''],['/ads',''],['/about',''],...CATS.map(k=>['/c/'+k[0],'']),...db.users.filter(u=>db.ads.some(a=>a.uid===u.id&&a.status==='active')).slice(-2000).map(u=>[sellerUrl(u),'']),...db.ads.filter(a=>a.status==='active').slice(-5000).map(a=>[adUrl(a),new Date(a.created).toISOString().slice(0,10)])];
      return out(c,200,`<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${us.map(([l,d])=>`<url><loc>${x(c.b+encodeURI(l))}</loc>${d?`<lastmod>${d}</lastmod>`:''}</url>`).join('')}</urlset>`,'application/xml; charset=utf-8')}
    if((r=p.match(/^\/uploads\/([a-f0-9]{20}\.(?:jpg|png|webp|gif))$/))){const f=path.join(UP,r[1]);if(!fs.existsSync(f)){res.writeHead(404);return res.end()}
      res.writeHead(200,{'Content-Type':{jpg:'image/jpeg',png:'image/png',webp:'image/webp',gif:'image/gif'}[r[1].split('.')[1]],'Cache-Control':'public,max-age=2592000,immutable'});return fs.createReadStream(f).pipe(res)}
    if(p==='/')return home(c);
    if(p==='/ads')return listPage(c);
    if((r=p.match(/^\/c\/([a-z-]+)$/))&&CM[r[1]])return listPage(c,r[1]);
    if((r=p.match(/^\/ad\/(\d+)(?:-.*)?$/))){const a=db.ads.find(x=>x.id===+r[1]);if(!a||(a.status==='hidden'&&!isAdmin(req)))return nf404(c);if(p!==adUrl(a))return redirect(c,adUrl(a),null,301);return adPage(c,a)}
    if(p==='/about')return html(c,200,{title:'من نحن',desc:`تعرّف على ${SITE}، منصة الإعلانات المبوبة العربية المجانية.`,path:'/about'},`<main class="w"><h1 style="font-size:26px;margin:20px 0">من نحن</h1><div class="box"><p>${E(SITE)} منصة عربية للإعلانات المبوبة المجانية. نربط بين البائعين والمشترين في كل الدول العربية بطريقة بسيطة وآمنة. أضف إعلانك مجانًا، وتصفح العروض في السيارات والعقارات والموبايلات والأثاث والوظائف والخدمات.</p><p style="margin-top:10px">نصيحة أمان: لا تحوّل أموالًا قبل معاينة السلعة، وقابل البائع في مكان عام.</p></div></main>`);
    if(p==='/login'||p==='/register'){if(c.user)return redirect(c,'/');return html(c,200,{title:p==='/login'?'تسجيل الدخول':'إنشاء حساب جديد',noindex:1,path:p},authForm(p==='/register',{},'',safeNext(u.searchParams.get('next'))))}
    if(p==='/post'){if(!c.user)return redirect(c,'/login?next=/post');if(roleOf(c.user)!=='seller')return html(c,200,{title:'أضف إعلانًا',noindex:1,path:'/post'},buyerGate());return html(c,200,{title:'أضف إعلانًا',noindex:1,path:'/post'},postForm({country:'eg',vodafone:c.user.vodafone||'',instapay:c.user.instapay||'',cod:c.user.cod!==false},'',!c.user.country))}
    if(p==='/my-ads')return redirect(c,'/dashboard',null,301);
    if(p==='/dashboard'){if(!c.user)return redirect(c,'/login?next=/dashboard');return dashboard(c)}
    if((r=p.match(/^\/buy\/(\d+)$/))){if(!c.user)return redirect(c,'/login?next='+p);const a=db.ads.find(x=>x.id===+r[1]);if(!buyable(c,a))return nf404(c);return html(c,200,{title:'إتمام الشراء',noindex:1,path:p},buyForm(a,{name:c.user.name}))}
    if((r=p.match(/^\/order\/(\d+)(\/pay)?$/))){if(!c.user)return redirect(c,'/login?next='+p);const o=myOrderOf(c,r[1]);if(!o)return nf404(c);
      if(r[2]){if(o.uid!==c.user.id||o.method==='cod'||o.status!=='new')return redirect(c,`/order/${o.id}`);return html(c,200,{title:'إتمام الدفع',noindex:1,path:p},payPage(o))}
      return html(c,200,{title:`طلب رقم ${o.id}`,noindex:1,path:p},orderPage(c,o))}
    if(p==='/my-orders'){if(!c.user)return redirect(c,'/login?next=/my-orders');return myOrders(c)}
    if((r=p.match(/^\/seller\/(\d+)(?:-.*)?$/))){const s=db.users.find(x=>x.id===+r[1]);if(!s)return nf404(c);if(p!==sellerUrl(s))return redirect(c,sellerUrl(s),null,301);return sellerPage(c,s)}
    if(p==='/profile'){if(!c.user)return redirect(c,'/login?next=/profile');if(roleOf(c.user)!=='seller')return redirect(c,'/dashboard');return html(c,200,{title:'صفحتي كبائع',noindex:1,path:p},profileForm(c.user))}
    if(p==='/chats'){if(!c.user)return redirect(c,'/login?next=/chats');return chatsPage(c)}
    if((r=p.match(/^\/chat\/(\d+)$/))){if(!c.user)return redirect(c,'/login?next='+p);const cv=myConv(c.user,r[1]);if(!cv)return nf404(c);return chatPage(c,cv)}
    if((r=p.match(/^\/api\/chat\/(\d+)\/msgs$/))){if(!c.user)return json(c,401,{error:'سجّل الدخول أولًا'});const cv=myConv(c.user,r[1]);if(!cv)return json(c,404,{error:'المحادثة غير موجودة'});return json(c,200,{msgs:newMsgs(c.user,cv,+u.searchParams.get('after')||0)})}
    if(p==='/admin/backup'){if(!isAdmin(req))return redirect(c,'/admin');save();const tmp=path.join(os.tmpdir(),'souqi-'+crypto.randomBytes(6).toString('hex')+'.db');
      try{SQL.exec(`VACUUM INTO '${tmp.replace(/'/g,"''")}'`);const buf=fs.readFileSync(tmp);res.writeHead(200,{'Content-Type':'application/octet-stream','Content-Disposition':`attachment; filename="souqi-backup-${new Date().toISOString().slice(0,10)}.db"`,'Content-Length':buf.length,'Cache-Control':'no-store'});return res.end(buf)}finally{try{fs.unlinkSync(tmp)}catch{}}}
    if(p==='/admin')return isAdmin(req)?adminPage(c):adminLogin(c);
    return nf404(c);
  }
  if(m==='POST'){
    if(p==='/api/upload'||p==='/api/admin-upload'){const adm=p==='/api/admin-upload';if(adm?!isAdmin(req):!c.user)return json(c,401,{error:'سجّل الدخول أولًا'});if(limited(adm?'ua':'u'+c.user.id,60,36e5))return json(c,429,{error:'تجاوزت حد الرفع، حاول لاحقًا'});
      let f;try{f=await raw(req,3e6)}catch{return json(c,413,{error:'حجم الصورة أكبر من 3 ميجا'})}const h4=f.slice(0,4).toString('latin1'),ext=h4==='\x89PNG'?'png':(f[0]===0xff&&f[1]===0xd8&&f[2]===0xff)?'jpg':h4==='GIF8'?'gif':(h4==='RIFF'&&f.slice(8,12).toString()==='WEBP')?'webp':'';
      if(!ext)return json(c,400,{error:'الصيغة غير مدعومة (PNG / JPG / WEBP / GIF فقط)'});const name=crypto.randomBytes(10).toString('hex')+'.'+ext;fs.writeFileSync(path.join(UP,name),f);return json(c,201,{url:'/uploads/'+name})}
    if(p==='/role'){if(!c.user)return redirect(c,'/login');const b=await form(req);c.user.role=b.role==='buyer'?'buyer':'seller';save();return redirect(c,b.to==='/post'&&c.user.role==='seller'?'/post':'/dashboard?m='+(c.user.role==='seller'?'تم تحويل حسابك إلى بائع':'تم تحويل حسابك إلى مشتري'))}
    if(p==='/logout'){const k=endSess(req,'sid');return redirect(c,'/',k)}
    if(p==='/register'){const b=await form(req),v={name:String(b.name||'').trim(),email:String(b.email||'').trim().toLowerCase(),role:b.role==='buyer'?'buyer':'seller',country:b.country},pw=String(b.password||''),bad=e=>html(c,400,{title:'إنشاء حساب جديد',noindex:1,path:'/register'},authForm(1,v,e,safeNext(b.next)));
      if(limited('r'+ip,10,9e5))return bad('محاولات كثيرة، حاول بعد 15 دقيقة');if(v.name.length<2||v.name.length>40)return bad('الاسم يجب أن يكون بين 2 و40 حرفًا');
      if(!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v.email)||v.email.length>120)return bad('البريد الإلكتروني غير صحيح');if(pw.length<8||pw.length>100)return bad('كلمة المرور يجب أن تكون 8 أحرف على الأقل');
      if(!KM[v.country])return bad('اختر البلد');if(pw!==b.password2)return bad('كلمتا المرور غير متطابقتين');if(db.users.some(x=>x.email===v.email))return bad('هذا البريد مسجّل بالفعل، جرّب تسجيل الدخول');
      const salt=crypto.randomBytes(16).toString('hex'),us={id:db.next++,name:v.name,email:v.email,salt,hash:hash(pw,salt),role:v.role,country:v.country,created:Date.now()};db.users.push(us);return redirect(c,v.role==='seller'&&!(b.next&&b.next!=='/')?'/dashboard':safeNext(b.next),startSess(req,'u',us.id))}
    if(p==='/login'){const b=await form(req),em=String(b.email||'').trim().toLowerCase(),x=db.users.find(y=>y.email===em),bad=(e,code=401)=>html(c,code,{title:'تسجيل الدخول',noindex:1,path:'/login'},authForm(0,{email:em},e,safeNext(b.next)));
      if(limited('l'+ip,10,9e5))return bad('محاولات كثيرة، حاول بعد 15 دقيقة',429);const ok=hash(String(b.password||''),x?x.salt:'0'.repeat(32))===(x?x.hash:'');
      if(!x||!ok)return bad('البريد أو كلمة المرور غير صحيحة');tries.delete('l'+ip);return redirect(c,safeNext(b.next),startSess(req,'u',x.id))}
    if(p==='/post'){if(!c.user)return redirect(c,'/login?next=/post');if(roleOf(c.user)!=='seller')return html(c,403,{title:'أضف إعلانًا',noindex:1,path:'/post'},buyerGate());const b=await form(req),v={title:String(b.title||'').trim(),cat:b.cat,country:c.user.country||b.country,city:String(b.city||'').trim(),price:String(b.price||'').trim(),phone:String(b.phone||'').trim(),desc:String(b.desc||'').trim(),images:String(b.images||''),...cleanPay(b)},bad=e=>html(c,400,{title:'أضف إعلانًا',noindex:1,path:'/post'},postForm(v,e,!c.user.country));
      if(v.title.length<5||v.title.length>100)return bad('العنوان يجب أن يكون بين 5 و100 حرف');if(!CM[v.cat])return bad('اختر القسم');if(!KM[v.country])return bad('اختر الدولة');if(v.city.length>40)return bad('اسم المدينة طويل');
      const pr=v.price===''?0:Number(v.price);if(!Number.isFinite(pr)||pr<0||pr>1e9)return bad('السعر غير صحيح');const ph=v.phone.replace(/[^\d+]/g,'');if(ph.length<7||ph.length>16)return bad('رقم التواصل غير صحيح');
      if(v.desc.length<20||v.desc.length>5000)return bad('الوصف يجب أن يكون بين 20 و5000 حرف');const imgs=v.images.split(',').filter(i=>/^\/uploads\/[a-f0-9]{20}\.(jpg|png|webp|gif)$/.test(i)&&fs.existsSync(path.join(UP,i.slice(9)))).slice(0,4);
      if(db.ads.filter(a=>a.uid===c.user.id&&Date.now()-a.created<864e5).length>=15)return bad('وصلت للحد اليومي (15 إعلانًا)');
      {const pe=payErr(v);if(pe)return bad(pe)}const a={id:db.nextAd++,uid:c.user.id,title:v.title,desc:v.desc,cat:v.cat,country:v.country,city:v.city,price:Math.round(pr),phone:ph,images:imgs,pay:{vodafone:v.vodafone,instapay:v.instapay,cod:v.cod},status:'active',created:Date.now(),views:0};db.ads.push(a);Object.assign(c.user,{vodafone:v.vodafone,instapay:v.instapay,cod:v.cod},c.user.country?{}:{country:v.country});save();return redirect(c,adUrl(a))}
    if((r=p.match(/^\/ad\/(\d+)\/(sold|delete)$/))){if(!c.user)return redirect(c,'/login');const a=db.ads.find(x=>x.id===+r[1]);if(!a||a.uid!==c.user.id){res.writeHead(403);return res.end('Forbidden')}
      if(r[2]==='sold'){if(a.status!=='hidden')a.status=a.status==='sold'?'active':'sold'}else db.ads=db.ads.filter(x=>x!==a);save();return redirect(c,'/dashboard')}
    if((r=p.match(/^\/buy\/(\d+)$/))){if(!c.user)return redirect(c,'/login?next='+p);const a=db.ads.find(x=>x.id===+r[1]);if(!buyable(c,a))return nf404(c);
      const b=await form(req),v={name:String(b.name||'').trim(),phone:String(b.phone||'').trim(),addr:String(b.addr||'').trim(),method:b.method},bad=e=>html(c,400,{title:'إتمام الشراء',noindex:1,path:p},buyForm(a,v,e)),ph=v.phone.replace(/[^\d+]/g,'');
      if(v.name.length<2||v.name.length>60)return bad('الاسم يجب أن يكون بين 2 و60 حرفًا');if(ph.length<7||ph.length>16)return bad('رقم الموبايل غير صحيح');if(v.addr.length<10||v.addr.length>300)return bad('اكتب عنوان التسليم بالتفصيل');if(!pms(a).includes(v.method))return bad('اختر طريقة الدفع');
      if(limited('o'+c.user.id,10,36e5))return bad('طلبات كثيرة، حاول لاحقًا');
      const o={id:db.nextOrder++,adId:a.id,title:a.title,amt:a.price,cur:KM[a.country][2],uid:c.user.id,sid:a.uid,name:v.name,phone:ph,addr:v.addr,method:v.method,to:v.method==='vodafone'?payOf(a).vodafone:v.method==='instapay'?payOf(a).instapay:'',sname:(db.users.find(x=>x.id===a.uid)||{}).name||'',status:'new',ref:'',dep:v.method==='vodafone'?Math.max(1,Math.round(a.price*DEP)):0,created:Date.now()};db.orders.push(o);save();return redirect(c,v.method==='cod'?`/order/${o.id}`:`/order/${o.id}/pay`)}
    if((r=p.match(/^\/order\/(\d+)\/pay$/))){if(!c.user)return redirect(c,'/login');const o=myOrderOf(c,r[1]);if(!o||o.uid!==c.user.id||o.method==='cod'||o.status!=='new')return redirect(c,'/my-orders');
      const b=await form(req),ref=String(b.ref||'').trim();if(ref.length<4||ref.length>60)return html(c,400,{title:'إتمام الدفع',noindex:1,path:p},payPage(o,'اكتب رقم العملية أو رقم المحفظة (4 أحرف على الأقل)'));o.ref=ref;o.status='awaiting';save();return redirect(c,`/order/${o.id}`)}
    if(p==='/chat/start'){if(!c.user)return redirect(c,'/login');const id=startChat(c,await form(req));if(id===0)return out(c,429,'محاولات كثيرة، حاول لاحقًا','text/plain; charset=utf-8');return id?redirect(c,'/chat/'+id):redirect(c,'/chats')}
    if((r=p.match(/^\/chat\/(\d+)$/))){if(!c.user)return redirect(c,'/login');const cv=myConv(c.user,r[1]);if(!cv)return nf404(c);sendMsg(c.user,cv,(await form(req)).body);return redirect(c,'/chat/'+cv.id)}
    if((r=p.match(/^\/api\/chat\/(\d+)\/send$/))){if(!c.user)return json(c,401,{error:'سجّل الدخول أولًا'});const cv=myConv(c.user,r[1]);if(!cv)return json(c,404,{error:'المحادثة غير موجودة'});let b={};try{b=JSON.parse((await raw(req,8e3)).toString())}catch{return json(c,400,{error:'طلب غير صالح'})}
      const e=sendMsg(c.user,cv,b.body);return e?json(c,e[0],{error:e[1]}):json(c,201,{ok:1})}
    if(p==='/profile'){if(!c.user)return redirect(c,'/login?next=/profile');const b=await form(req),pay=cleanPay(b),name=String(b.name||'').trim(),city=String(b.city||'').trim(),country=KM[b.country]?b.country:(c.user.country||'eg'),bio=String(b.bio||'').replace(/\r\n/g,'\n').trim(),u=c.user,
      bad=e=>html(c,400,{title:'صفحتي كبائع',noindex:1,path:'/profile'},profileForm({...u,name,city,country,bio,...pay},e));
      if(name.length<2||name.length>40)return bad('الاسم يجب أن يكون بين 2 و40 حرفًا');if(city.length>40)return bad('اسم المدينة طويل');if(bio.length>300)return bad('النبذة طويلة (الحد الأقصى 300 حرف)');const pe=payErr(pay);if(pe)return bad(pe);
      Object.assign(u,{name,city,country,bio},pay);save();return redirect(c,sellerUrl(u))}
    if((r=p.match(/^\/order\/(\d+)\/(paid|done|cancel)$/))){if(!c.user)return redirect(c,'/login');const o=db.orders.find(x=>x.id===+r[1]);
      if(!o||(o.sid!==c.user.id&&!(o.uid===c.user.id&&r[2]==='cancel'&&o.status==='new')))return redirect(c,'/my-orders');orderAct(o,r[2]);save();return redirect(c,'/order/'+o.id)}
    if((r=p.match(/^\/admin\/ad\/(\d+)\/toggle$/))){if(!isAdmin(req))return redirect(c,'/admin');const a=db.ads.find(x=>x.id===+r[1]);let m='الإعلان غير موجود';
      if(a){if(a.status==='hidden'){a.status=a.prev&&a.prev!=='hidden'?a.prev:'active';m='تم إظهار الإعلان'}else{a.prev=a.status;a.status='hidden';m='تم إخفاء الإعلان'}save()}return redirect(c,'/admin?tab=ads&m='+m)}
    if(p==='/admin/banner/add'){if(!isAdmin(req))return redirect(c,'/admin');const b=await form(req),slot=b.slot==='side'?'side':'top',img=String(b.img||'').trim(),link=String(b.link||'').trim(),code=String(b.code||'').trim().slice(0,5000),
      okImg=/^\/uploads\/[a-f0-9]{20}\.(jpg|png|webp|gif)$/.test(img)||/^https:\/\/[^\s"'<>]+$/.test(img),okLink=!link||/^https?:\/\/[^\s"'<>]+$/.test(link);
      if(!okLink||(!code&&!okImg))return redirect(c,'/admin?tab=banners&m=ارفع صورة أو اكتب كود إعلان، وتأكد أن الرابط يبدأ بـ https://');
      db.banners.push({id:db.nextBanner++,slot,img:code?'':img,link:code?'':link,code,on:true,created:Date.now()});save();return redirect(c,'/admin?tab=banners&m=تمت إضافة المساحة الإعلانية')}
    if((r=p.match(/^\/admin\/banner\/(\d+)\/(toggle|delete)$/))){if(!isAdmin(req))return redirect(c,'/admin');const b=db.banners.find(x=>x.id===+r[1]);if(b){if(r[2]==='toggle')b.on=!b.on;else db.banners=db.banners.filter(x=>x!==b);save()}return redirect(c,'/admin?tab=banners&m=تم')}
    if(p==='/admin/login'){const b=await form(req);if(limited('a'+ip,6,9e5))return adminLogin(c,'محاولات كثيرة، حاول لاحقًا');const a=db.admin;
      if(b.user!==a.user||hash(String(b.password||''),a.salt)!==a.hash)return adminLogin(c,'بيانات الدخول غير صحيحة');tries.delete('a'+ip);return redirect(c,'/admin',startSess(req,'a',0))}
    if(p==='/admin/logout')return redirect(c,'/admin',endSess(req,'asid'));
    if((r=p.match(/^\/admin\/(ad|user)\/(\d+)\/delete$/))){if(!isAdmin(req))return redirect(c,'/admin');const id=+r[2];
      if(r[1]==='ad')db.ads=db.ads.filter(a=>a.id!==id);else{db.users=db.users.filter(x=>x.id!==id);qr('DELETE FROM convs WHERE buyer_id=? OR seller_id=?',id,id);db.ads=db.ads.filter(a=>a.uid!==id);for(const k in db.sess)if(db.sess[k].k==='u'&&db.sess[k].uid===id)delete db.sess[k]}
      save();return redirect(c,`/admin?tab=${r[1]==='ad'?'ads':'users'}&m=${'تم الحذف'}`)}
  }
  nf404(c);
}catch(e){console.error(e);if(!res.headersSent){res.writeHead(500,{'Content-Type':'text/plain; charset=utf-8'});res.end('خطأ في الخادم')}}}).listen(PORT,()=>console.log(`${SITE} يعمل على http://localhost:${PORT}  |  لوحة التحكم: /admin`));
