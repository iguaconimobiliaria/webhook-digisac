const express = require('express');
const axios = require('axios');
const fs = require('fs');
const _ = require('lodash');
const sqlite3 = require('sqlite3').verbose();

const app = express();
app.use(express.json({ limit: '10mb' }));

// Configurações com suas credenciais reais
const CONFIG = {
  digisacApiUrl: 'https://iguaconimobiliaria.digisac.biz/api/v1',
  digisacToken: '753647f9a569909d1b0bed1f68117eca7c90cb7d',
  crmTokenUrl: 'https://api.si9sistemas.com.br/imobilsi9-api/oauth/token?username=iguacon2-integracao&password=bpaKN3yhH%2B9715T%249MMt&grant_type=password',
  crmApiUrl: 'https://api.si9sistemas.com.br/imobilsi9-api/lead',
  crmAuthHeader: 'Basic OTBlYTU4MjctZDQ2Zi00OGE1LTg1NjMtNzQ2YTlmMjBlZDZiOmEwZmU0MDhjLTlhNTQtNDRmMC1iN2I2LTBiMTk0Y2FhNjJlNQ==',
  port: process.env.PORT || 3000,
  bufferTime: 20000, // 20 segundos de acumulação
  maxRetries: 3,
  retryDelay: 2000,
  requestTimeout: 15000
};

// Mapeamentos de IDs - INCLUINDO KLEO
const USER_ID_MAPPING = {
  '64a82223-f414-4ad2-9275-eb20154de6dc': 87, // Cleusa
  'ec2b04ae-939b-4da5-90e0-f3702a007f5d': 1,  // Admin
  '48b2180f-55d2-4b4d-a9b7-9b583c7ac599': 77, // Suelin
  'ee5fa9f0-e203-4138-b0e1-48c6d23d3d3c': 49, // Lucas
  '9802485b-9f3e-45a9-91fa-891e37918e3d': 12, // Lucia
  'e97e9a59-72fb-4b10-a1e9-13eee1fbb1ea': 27,  // Kleo
  'b02852d3-b3df-4c61-ab5b-9feb4840c99e': 105, // Jaque
  '3a512b07-310b-42b9-bfb0-dc3905168c24': 118, // Flávia
  '16399312-d44d-4684-8250-2872be84f06c': 74, // Ana
  '106cf693-94f2-41e8-99fc-cac5bdff8ddd': 139, // Heloisa
  'd204e1a8-a31d-4c57-b340-6ddce2f4632e': 127, // Danilo
  '2eb20be5-6ace-4f18-91bb-48646e9a4bb4': 112, // Igor
  '3be51a36-a8d9-40b3-9f00-cbc6858a7e6b': 30, // Alexandre
  'f2826093-1382-420d-9d81-ba85f8018d65': 106, // Danielli
  'ce75886f-a5ab-4ff6-9719-37ec46edbcb6': 56, // Edson
  '931566e9-e7b9-4806-a24b-3dfbdd2f0892': 142, // Paulo
  'f9aaccc5-ad8c-4097-821c-4ca9d96e45ef': 55  // Viviane
};

const TAG_SOURCE_MAPPING = {
  '9bf06544-6e4b-4d96-903d-2c26e52ca0c1': 'Campanha Jd. Alice - IM. Terceiros',
  '11221ef6-8afc-4ab2-9414-d8fa7fac573a': 'Campanha Ilha Bella - IM. Terceiros',
  'c2fed5c7-92d8-43e4-b255-4d585178bd3e': 'Campanha Vila Maria - IM. Terceiros',
  'e71536ca-9f9c-4c5d-aa79-dfbee22ff332': 'Digisac - Imóveis de Terceiro',
  '2ebcc701-02a6-4516-8b50-711333c12db7': 'Campanha Araucaria',
  'c0dbb25e-bb8f-4c00-bc53-4f6077834954': 'Campanha Casa Claudir',
  '14295746-a737-4838-89ce-68a907b00eca': 'Campanha Itamaraty',
  '159b47e5-c0f6-42f8-a180-75db3066b40b': 'Campanha London',
  'cfe5372c-3d16-45cd-b57e-81239e726f21': 'Campanha Renoir',
  '75c153c4-e09c-4749-b610-bfcc30726043': 'Campanha Rigiero',
  '7a8469cd-4e63-4afc-88c9-7ee14eef6514': 'Campanha Águas Claras'
};

const EMAIL_CUSTOM_FIELD_ID = '1e9f04d2-2c6f-4020-9965-49a0b47d16ca';

// Cache para token do CRM (válido por 60 minutos)
let crmTokenCache = {
  token: null,
  expiresAt: null
};

const db = new sqlite3.Database('webhook.db', (err) => {
  if (err) {
    console.error('❌ Erro ao conectar ao banco:', err.message);
    process.exit(1);
  } else {
    console.log('✅ Banco de dados criado com sucesso');
    initializeDatabase();
  }
});

function initializeDatabase() {
  db.serialize(() => {
    // Tabela para dados enviados (para comparação futura) - CORRIGIDA COM INTERNAL_NAME
    db.run(`CREATE TABLE IF NOT EXISTS sent_data (
      id TEXT PRIMARY KEY,
      name TEXT,
      internal_name TEXT,
      number TEXT,
      note TEXT,
      timestamp TEXT,
      sent_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Tabela para logs de envio
    db.run(`CREATE TABLE IF NOT EXISTS send_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      contact_id TEXT,
      payload TEXT,
      status TEXT,
      response TEXT,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Tabela para cache de dados da API Digisac
    db.run(`CREATE TABLE IF NOT EXISTS digisac_cache (
      contact_id TEXT PRIMARY KEY,
      api_data TEXT,
      cached_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    console.log('✅ Banco de dados inicializado');
  });
}

// Buffer para acumular dados por 20 segundos
let dataBuffer = new Map();
let bufferTimer = null;

// Função para gerar token do CRM
async function getCrmToken() {
  try {
    // Verifica se o token ainda é válido (com margem de 5 minutos)
    const now = new Date();
    if (crmTokenCache.token && crmTokenCache.expiresAt && now < crmTokenCache.expiresAt) {
      console.log('🔑 Usando token CRM em cache');
      return crmTokenCache.token;
    }

    console.log('🔑 Gerando novo token CRM...');
    
    const response = await axios.post(CONFIG.crmTokenUrl, {}, {
      headers: {
        'Authorization': CONFIG.crmAuthHeader,
        'Content-Type': 'application/json'
      },
      timeout: CONFIG.requestTimeout
    });

    const token = response.data.access_token;
    
    if (!token) {
      throw new Error('Token não retornado pela API do CRM');
    }

    // Cache do token por 55 minutos (margem de segurança)
    crmTokenCache.token = token;
    crmTokenCache.expiresAt = new Date(now.getTime() + 55 * 60 * 1000);
    
    console.log('✅ Token CRM gerado com sucesso');
    return token;
    
  } catch (error) {
    console.error('❌ Erro ao gerar token CRM:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
      console.error('Response status:', error.response.status);
    }
    throw error;
  }
}

function validateWebhookData(req, res, next) {
  try {
    const { body } = req;
    
    if (!body || typeof body !== 'object') {
      return res.status(400).json({ error: 'Dados inválidos: body deve ser um objeto' });
    }
    
    if (!body.data || typeof body.data !== 'object') {
      return res.status(400).json({ error: 'Dados inválidos: data é obrigatório' });
    }
    
    if (!body.data.id) {
      return res.status(400).json({ error: 'Dados inválidos: data.id é obrigatório' });
    }
    
    next();
  } catch (error) {
    console.error('❌ Erro na validação:', error.message);
    res.status(500).json({ error: 'Erro interno na validação' });
  }
}

app.post('/webhook', validateWebhookData, async (req, res) => {
  const data = req.body;
  const timestamp = new Date().toISOString();
  
  console.log('📥 Dados recebidos e adicionados ao buffer:', {
    id: data.data.id,
    name: data.data.name,
    internalName: data.data.internalName,
    note: data.data.note,
    number: data.data.idFromService,
    timestamp: timestamp
  });

  try {
    // Adiciona ao buffer (sempre sobrescreve se for o mesmo ID)
    const contactId = data.data.id;
    
    // Extração inteligente dos campos da Digisac
    const extractedName = data.data.name || data.data.internalName || data.data.alternativeName || '';
    const extractedNumber = data.data.idFromService || data.data.data?.number || '';
    const extractedNote = data.data.note || '';
    
    const recordData = {
      id: contactId,
      name: extractedName,
      internalName: data.data.internalName,
      number: extractedNumber,
      note: extractedNote,
      timestamp: timestamp,
      originalData: data
    };
    
    dataBuffer.set(contactId, recordData);
    
    console.log(`📦 Buffer atualizado - Total de IDs únicos: ${dataBuffer.size}`);
    
    // Responde imediatamente
    res.status(200).json({ 
      status: 'success', 
      message: 'Dados adicionados ao buffer',
      timestamp: timestamp,
      bufferSize: dataBuffer.size
    });

    // Inicia timer se não estiver rodando
    if (!bufferTimer) {
      console.log(`⏰ Timer de ${CONFIG.bufferTime/1000} segundos iniciado`);
      bufferTimer = setTimeout(processBuffer, CONFIG.bufferTime);
    }
    
  } catch (error) {
    console.error('❌ Erro ao processar webhook:', error.message);
    res.status(500).json({ error: 'Erro interno do servidor' });
  }
});

// Função para buscar dados completos do contato na API Digisac
async function fetchContactFromDigisac(contactId) {
  try {
    console.log(`🔍 [FETCH] Buscando dados completos do contato ${contactId} na API Digisac`);
    
    const url = `${CONFIG.digisacApiUrl}/contacts/${contactId}?include[0]=customFieldValues&include[1]=tags`;
    console.log(`🔗 [FETCH] URL da requisição: ${url}`);
    console.log(`🔑 [FETCH] Token: ${CONFIG.digisacToken ? 'PRESENTE' : 'AUSENTE'}`);
    
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${CONFIG.digisacToken}`,
        'Content-Type': 'application/json'
      },
      timeout: CONFIG.requestTimeout
    });

    console.log(`✅ [FETCH] Dados do contato ${contactId} obtidos com sucesso`);
    console.log(`📊 [FETCH] Status da resposta: ${response.status}`);
    console.log(`📊 [FETCH] CustomFieldValues encontrados: ${response.data?.customFieldValues?.length || 0}`);
    
    // Cache dos dados para evitar consultas desnecessárias
    await new Promise((resolve, reject) => {
      db.run(
        'INSERT OR REPLACE INTO digisac_cache (contact_id, api_data) VALUES (?, ?)',
        [contactId, JSON.stringify(response.data)],
        function(err) {
          if (err) reject(err);
          else resolve(this.changes);
        }
      );
    });
    
    return response.data;
    
  } catch (error) {
    console.error(`❌ [FETCH] ERRO CRÍTICO ao buscar contato ${contactId} na API Digisac:`);
    console.error(`📊 [FETCH] Tipo do erro:`, error.constructor.name);
    console.error(`📊 [FETCH] Mensagem:`, error.message);
    console.error(`📊 [FETCH] Stack trace:`, error.stack);
    if (error.response) {
      console.error(`📊 [FETCH] Response status:`, error.response.status);
      console.error(`📊 [FETCH] Response data:`, error.response.data);
    } else if (error.request) {
      console.error(`📊 [FETCH] Request sem resposta:`, error.request);
    }
    throw error;
  }
}

// Função para buscar tickets do contato na API Digisac
async function fetchContactTickets(contactId) {
  try {
    console.log(`🎫 Buscando tickets do contato ${contactId} na API Digisac`);
    
    const url = `${CONFIG.digisacApiUrl}/tickets?where[contactId]=${contactId}`;
    
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${CONFIG.digisacToken}`,
        'Content-Type': 'application/json'
      },
      timeout: CONFIG.requestTimeout
    });

    console.log(`✅ Tickets do contato ${contactId} obtidos:`, {
      total: response.data.total,
      quantidade: response.data.data ? response.data.data.length : 0
    });
    
    return response.data.data || []; // Retorna array de tickets
    
  } catch (error) {
    console.error(`❌ Erro ao buscar tickets do contato ${contactId}:`, error.message);
    if (error.response) {
      console.error('Response status:', error.response.status);
      console.error('Response data:', error.response.data);
    }
    return []; // Retorna array vazio em caso de erro
  }
}

// Função para extrair email do campo personalizado
function extractEmailFromCustomFields(customFieldValues) {
  if (!customFieldValues || !Array.isArray(customFieldValues)) {
    return ''; // CORRIGIDO: retorna vazio
  }
  
  const emailField = customFieldValues.find(field => 
    field.customFieldId === EMAIL_CUSTOM_FIELD_ID
  );
  
  // Verifica se o campo existe E se tem valor não vazio
  if (emailField && emailField.value && emailField.value.trim() !== '') {
    return emailField.value.trim();
  }
  
  return ''; // CORRIGIDO: retorna vazio se não houver email
}

// Função para extrair source das tags
function extractSourceFromTags(tags) {
  if (!tags || !Array.isArray(tags)) {
    return 'Digisac - Imóveis de Terceiro';
  }
  
  // Procura por tags conhecidas
  for (const tag of tags) {
    if (TAG_SOURCE_MAPPING[tag.id]) {
      return TAG_SOURCE_MAPPING[tag.id];
    }
  }
  
  // Se não encontrou nenhuma tag conhecida, retorna padrão
  return 'Digisac - Imóveis de Terceiro';
}

// Função para extrair user ID dos tickets
function extractUserIdFromTickets(tickets) {
  console.log('🎫 ANALISANDO TICKETS:', {
    temTickets: !!tickets,
    ehArray: Array.isArray(tickets),
    quantidade: tickets ? tickets.length : 0
  });
  
  if (!tickets || !Array.isArray(tickets) || tickets.length === 0) {
    console.log('⚠️ SEM TICKETS - Usando Admin (ID: 1)');
    return 1; // Admin como padrão
  }
  
  // Pega o ticket mais recente
  const latestTicket = tickets.sort((a, b) => 
    new Date(b.createdAt) - new Date(a.createdAt)
  )[0];
  
  console.log('🎫 TICKET MAIS RECENTE:', {
    ticketId: latestTicket.id,
    userId: latestTicket.userId,
    createdAt: latestTicket.createdAt,
    status: latestTicket.status
  });
  
  const digisacUserId = latestTicket.userId;
  const mappedUserId = USER_ID_MAPPING[digisacUserId];
  
  console.log('🔄 MAPEAMENTO USER ID:', {
    digisacUserId: digisacUserId,
    mappedUserId: mappedUserId,
    fallback: mappedUserId || 1
  });
  
  console.log('📋 MAPEAMENTO DISPONÍVEL:', USER_ID_MAPPING);
  
  return mappedUserId || 1; // Admin como fallback
}

// Função para formatar telefone (apenas números limpos - máximo 11 caracteres)
function formatPhoneNumber(number) {
  if (!number) return { cellNumber: '', phoneNumber: '', internationalPhoneNumber: '' };
  
  // Remove TODOS os caracteres não numéricos (incluindo espaços, parênteses, hífens, etc.)
  const cleanNumber = number.replace(/[^\d]/g, '').trim();
  
  console.log(`🔍 FORMATAÇÃO TELEFONE:`, {
    original: number,
    limpo: cleanNumber,
    tamanho: cleanNumber.length
  });
  
  // Lista de DDDs brasileiros válidos
  const brazilianDDDs = [
    '11', '12', '13', '14', '15', '16', '17', '18', '19', // SP
    '21', '22', '24', // RJ
    '27', '28', // ES
    '31', '32', '33', '34', '35', '37', '38', // MG
    '41', '42', '43', '44', '45', '46', // PR
    '47', '48', '49', // SC
    '51', '53', '54', '55', // RS
    '61', // DF
    '62', '64', // GO
    '63', // TO
    '65', '66', // MT
    '67', // MS
    '68', // AC
    '69', // RO
    '71', '73', '74', '75', '77', // BA
    '79', // SE
    '81', '87', // PE
    '82', // AL
    '83', // PB
    '84', // RN
    '85', '88', // CE
    '86', '89', // PI
    '91', '93', '94', // PA
    '92', '97', // AM
    '95', // RR
    '96', // AP
    '98', '99' // MA
  ];
  
  let brazilianNumber = '';
  let isBrazilian = false;
  
  // Verifica se é número brasileiro (código 55)
  if (cleanNumber.startsWith('55') && cleanNumber.length >= 12) {
    brazilianNumber = cleanNumber.substring(2); // Remove o 55
    isBrazilian = true;
    console.log(`📱 NÚMERO BRASILEIRO (com código 55):`, {
      semCodigo: brazilianNumber,
      tamanho: brazilianNumber.length
    });
  }
  // Verifica se é número brasileiro (sem código 55, mas com DDD brasileiro)
  else if (cleanNumber.length >= 10 && cleanNumber.length <= 11) {
    const possibleDDD = cleanNumber.substring(0, 2);
    if (brazilianDDDs.includes(possibleDDD)) {
      brazilianNumber = cleanNumber;
      isBrazilian = true;
      console.log(`📱 NÚMERO BRASILEIRO (sem código 55, DDD ${possibleDDD}):`, {
        numero: brazilianNumber,
        tamanho: brazilianNumber.length,
        ddd: possibleDDD
      });
    }
  }
  
  if (isBrazilian) {
    // Retorna apenas números limpos (sem formatação) - FORÇA LIMITE DE 11
    if (brazilianNumber.length >= 11) {
      const finalNumber = brazilianNumber.substring(0, 11); // FORÇA máximo 11
      console.log(`✅ NÚMERO FINAL:`, {
        numero: finalNumber,
        tamanho: finalNumber.length
      });
      return {
        cellNumber: finalNumber,
        phoneNumber: finalNumber,
        internationalPhoneNumber: ''
      };
    } else if (brazilianNumber.length === 10) {
      console.log(`✅ NÚMERO FINAL (10 dígitos):`, {
        numero: brazilianNumber,
        tamanho: brazilianNumber.length
      });
      return {
        cellNumber: brazilianNumber,
        phoneNumber: brazilianNumber,
        internationalPhoneNumber: ''
      };
    }
    
    // Fallback - força limite
    const finalNumber = brazilianNumber.substring(0, 11);
    console.log(`⚠️ FALLBACK - NÚMERO FINAL:`, {
      numero: finalNumber,
      tamanho: finalNumber.length
    });
    return {
      cellNumber: finalNumber,
      phoneNumber: finalNumber,
      internationalPhoneNumber: ''
    };
  }
  
  // Se não é brasileiro, coloca no campo internacional (limitado)
  const limitedNumber = cleanNumber.substring(0, 11);
  console.log(`🌍 NÚMERO INTERNACIONAL:`, {
    numero: limitedNumber,
    tamanho: limitedNumber.length
  });
  return {
    cellNumber: '',
    phoneNumber: '',
    internationalPhoneNumber: `+${limitedNumber}`
  };
}

// Função para escolher o nome preferido (internalName tem prioridade se não estiver vazio)
function getPreferredName(contactData, digisacApiData) {
  // CORRIGIDO: Usa dados do webhook primeiro
  if (contactData.internalName && contactData.internalName.trim() !== '') {
    console.log(`📝 USANDO INTERNAL NAME DO WEBHOOK: "${contactData.internalName}" (em vez de "${contactData.name}")`);
    return contactData.internalName.trim();
  }
  
  // Fallback para API se não houver no webhook
  if (digisacApiData.internalName && digisacApiData.internalName.trim() !== '') {
    console.log(`📝 USANDO INTERNAL NAME DA API: "${digisacApiData.internalName}" (em vez de "${contactData.name}")`);
    return digisacApiData.internalName.trim();
  }
  
  // Fallback para o name original
  console.log(`📝 USANDO NAME ORIGINAL: "${contactData.name}"`);
  return contactData.name;
}

// Função para verificar se o note mudou e retornar valor apropriado
function getObservationValue(currentNote, lastSentNote) {
  // Se não há note atual, retorna vazio
  if (!currentNote || currentNote.trim() === '') {
    console.log(`📝 NOTE VAZIO - Enviando observation vazia`);
    return '';
  }
  
  // Se o note é igual ao último enviado, retorna vazio para não duplicar
  if (currentNote === lastSentNote) {
    console.log(`📝 NOTE IGUAL AO ANTERIOR - Enviando observation vazia para evitar duplicação`);
    console.log(`📝 Note atual: "${currentNote}"`);
    console.log(`📝 Note anterior: "${lastSentNote}"`);
    return '';
  }
  
  // Se o note mudou, envia o novo valor
  console.log(`📝 NOTE ALTERADO - Enviando nova observation`);
  console.log(`📝 Note anterior: "${lastSentNote}"`);
  console.log(`📝 Note atual: "${currentNote}"`);
  return currentNote;
}

// Função para transformar dados para formato do CRM
async function transformToCrmFormat(contactData, digisacApiData, contactTickets) {
  try {
    const { cellNumber, phoneNumber, internationalPhoneNumber } = formatPhoneNumber(contactData.number);
    const email = extractEmailFromCustomFields(digisacApiData.customFieldValues);
    const source = extractSourceFromTags(digisacApiData.tags);
    const userId = extractUserIdFromTickets(contactTickets);
    
    // Usar o ID real do contato da Digisac
    const contactId = contactData.id;
    
    // Buscar dados anteriores para comparação do note
    const lastSentData = await new Promise((resolve, reject) => {
      db.get(
        'SELECT * FROM sent_data WHERE id = ?',
        [contactId],
        (err, row) => {
          if (err) reject(err);
          else resolve(row);
        }
      );
    });
    
    // Extrair dados reais do usuário/atendente dos tickets
    let userData = {
      id: userId,
      username: "elliot", // Fixo conforme solicitado
      email: "elliot@email.com", // Fixo
      name: "Elliot Alderson" // Fixo
    };
    
    const observationValue = getObservationValue(contactData.note, lastSentData?.note);
    
    const crmPayload = {
  name: getPreferredName(contactData, digisacApiData),
  classification: "High",
  interestedIn: "buy",
  source: source,
  cellNumber: cellNumber,
  phoneNumber: phoneNumber,
  internationalPhoneNumber: internationalPhoneNumber,
  email: email,
  user: userData,
  contacts: [
    {
      propertyId: 123,
      observation: "Lead processado automaticamente via Digisac",
      contactType: 11,
      date: new Date().toISOString().substring(0, 10) + ':' + new Date().toISOString().substring(11, 19)
    }
  ]
};

// Adiciona observation apenas se não estiver vazio
if (observationValue && observationValue.trim() !== '') {
  crmPayload.observation = observationValue;
  crmPayload.observationLead = observationValue.substring(0, 150);
}

    
    console.log(`🔄 Dados transformados para CRM:`, {
  contactId: contactId,
  name: crmPayload.name,
  classification: crmPayload.classification,
  source: crmPayload.source,
  cellNumber: crmPayload.cellNumber,
  phoneNumber: crmPayload.phoneNumber,
  internationalPhoneNumber: crmPayload.internationalPhoneNumber,
  email: crmPayload.email,
  userId: crmPayload.user.id,
  hasObservation: !!crmPayload.observation,  // ← CORRIGIDO
  observationLength: crmPayload.observation ? crmPayload.observation.length : 0  // ← CORRIGIDO
});

    
    return crmPayload;
    
  } catch (error) {
    console.error(`❌ Erro ao transformar dados para CRM:`, error.message);
    throw error;
  }
}

// Função para enviar dados para o CRM
async function sendToCrm(contactData, crmPayload) {
  let retryCount = 0;
  
  while (retryCount < CONFIG.maxRetries) {
    try {
      const token = await getCrmToken();
      
      console.log(`🚀 Enviando dados para CRM - Contato: ${contactData.id}`);
      
      const response = await axios.post(CONFIG.crmApiUrl, crmPayload, {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        timeout: CONFIG.requestTimeout
      });
      
      console.log(`✅ Dados enviados com sucesso para CRM - Contato: ${contactData.id}`);
      console.log(`📊 Response status: ${response.status}`);
      
      // Salva no banco para comparação futura - INCLUINDO INTERNAL_NAME
      await new Promise((resolve, reject) => {
        db.run(
          'INSERT OR REPLACE INTO sent_data (id, name, internal_name, number, note, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
          [contactData.id, contactData.name, contactData.internalName, contactData.number, contactData.note, contactData.timestamp],
          function(err) {
            if (err) reject(err);
            else resolve(this.changes);
          }
        );
      });
      
      // Log de sucesso
      await new Promise((resolve, reject) => {
        db.run(
          'INSERT INTO send_logs (contact_id, payload, status, response) VALUES (?, ?, ?, ?)',
          [contactData.id, JSON.stringify(crmPayload), 'success', JSON.stringify(response.data)],
          function(err) {
            if (err) reject(err);
            else resolve(this.changes);
          }
        );
      });
      
      return response.data;
      
    } catch (error) {
      retryCount++;
      console.error(`❌ Erro ao enviar para CRM - Contato: ${contactData.id}:`, error.message);
      
      if (error.response) {
        console.error('Response status:', error.response.status);
        console.error('Response data:', error.response.data);
      }
      
      // Log de erro
      await new Promise((resolve, reject) => {
        db.run(
          'INSERT INTO send_logs (contact_id, payload, status, response) VALUES (?, ?, ?, ?)',
          [contactData.id, JSON.stringify(crmPayload), 'error', error.message],
          function(err) {
            if (err) reject(err);
            else resolve(this.changes);
          }
        );
      });
      
      if (retryCount < CONFIG.maxRetries) {
        console.log(`🔄 Tentativa ${retryCount}/${CONFIG.maxRetries} - Aguardando ${CONFIG.retryDelay}ms antes de tentar novamente`);
        await new Promise(resolve => setTimeout(resolve, CONFIG.retryDelay));
      } else {
        throw error;
      }
    }
  }
}

// Função para verificar se houve mudanças nos campos principais
async function hasFieldsChanged(currentData) {
  try {
    const lastSentData = await new Promise((resolve, reject) => {
      db.get(
        'SELECT * FROM sent_data WHERE id = ?',
        [currentData.id],
        (err, row) => {
          if (err) reject(err);
          else resolve(row);
        }
      );
    });
    
    if (!lastSentData) {
      console.log(`🆕 PRIMEIRO ENVIO - Contato: ${currentData.id}`);
      return true;
    }
    
    // Verifica mudanças nos campos principais (incluindo internalName)
    const fieldsChanged = 
      currentData.name !== lastSentData.name ||
      currentData.internalName !== lastSentData.internal_name || // CORRIGIDO: comparação do internalName
      currentData.number !== lastSentData.number ||
      currentData.note !== lastSentData.note;
    
    if (fieldsChanged) {
      console.log(`🔄 CAMPOS PRINCIPAIS ALTERADOS - Contato ${currentData.id}:`, {
        name: { anterior: lastSentData.name, atual: currentData.name },
        internalName: { anterior: lastSentData.internal_name, atual: currentData.internalName },
        number: { anterior: lastSentData.number, atual: currentData.number },
        note: { anterior: lastSentData.note, atual: currentData.note }
      });
    }
    
    return fieldsChanged;
    
  } catch (error) {
    console.error(`❌ Erro ao verificar mudanças:`, error.message);
    return true; // Em caso de erro, assume que houve mudança
  }
}

// Função para verificar mudanças em campos personalizados
async function hasCustomFieldsChanged(contactId, currentApiData) {
  try {
    const cachedData = await new Promise((resolve, reject) => {
      db.get(
        'SELECT api_data FROM digisac_cache WHERE contact_id = ?',
        [contactId],
        (err, row) => {
          if (err) reject(err);
          else resolve(row);
        }
      );
    });
    
    console.log('🔍 VERIFICANDO CAMPOS PERSONALIZADOS:', {
      temCurrentApiData: !!currentApiData,
      temPreviousApiData: !!cachedData,
      currentCustomFields: currentApiData?.customFieldValues?.length || 0,
      previousCustomFields: cachedData ? JSON.parse(cachedData.api_data)?.customFieldValues?.length || 0 : 0
    });
    
    if (!cachedData) {
      console.log('🆕 PRIMEIRO CACHE - Assumindo mudança nos campos personalizados');
      return true;
    }
    
    const previousApiData = JSON.parse(cachedData.api_data);
    
    // Compara campos personalizados
    const currentCustomFields = currentApiData.customFieldValues || [];
    const previousCustomFields = previousApiData.customFieldValues || [];
    
    console.log('📋 DADOS ATUAIS DA API:', {
      customFields: currentCustomFields.map(field => ({
        id: field.customFieldId,
        value: field.value
      }))
    });
    
    console.log('📋 DADOS ANTERIORES DO CACHE:', {
      customFields: previousCustomFields.map(field => ({
        id: field.customFieldId,
        value: field.value
      }))
    });
    
    // Cria mapas para comparação mais fácil
    const currentFieldsMap = new Map();
    const previousFieldsMap = new Map();
    
    currentCustomFields.forEach(field => {
      const fieldName = field.customFieldId === EMAIL_CUSTOM_FIELD_ID ? 'Email' : `Campo${field.customFieldId.slice(-1)}`;
      currentFieldsMap.set(fieldName, field.value || '');
    });
    
    previousCustomFields.forEach(field => {
      const fieldName = field.customFieldId === EMAIL_CUSTOM_FIELD_ID ? 'Email' : `Campo${field.customFieldId.slice(-1)}`;
      previousFieldsMap.set(fieldName, field.value || '');
    });
    
    console.log('📋 MAPA CAMPOS ATUAIS:', Array.from(currentFieldsMap.entries()));
    console.log('📋 MAPA CAMPOS ANTERIORES:', Array.from(previousFieldsMap.entries()));
    
    // Verifica se algum campo mudou
    let hasChanges = false;
    
    // Verifica campos atuais
    for (const [fieldName, currentValue] of currentFieldsMap) {
      const previousValue = previousFieldsMap.get(fieldName) || '';
      console.log(`🔍 COMPARANDO CAMPO "${fieldName}": atual="${currentValue}" vs anterior="${previousValue}"`);
      
      if (currentValue !== previousValue) {
        console.log(`🔄 CAMPO PERSONALIZADO ALTERADO: ${fieldName}`);
        hasChanges = true;
      }
    }
    
    // Verifica campos que foram removidos
    for (const [fieldName, previousValue] of previousFieldsMap) {
      if (!currentFieldsMap.has(fieldName)) {
        console.log(`🗑️ CAMPO PERSONALIZADO REMOVIDO: ${fieldName}`);
        hasChanges = true;
      }
    }
    
    if (!hasChanges) {
      console.log('ℹ️ NENHUM CAMPO PERSONALIZADO ALTERADO');
    }
    
    return hasChanges;
    
  } catch (error) {
    console.error(`❌ Erro ao verificar mudanças em campos personalizados:`, error.message);
    return true; // Em caso de erro, assume que houve mudança
  }
}

// Função para processar um contato individual
async function processContact(contactData) {
  try {
    console.log(`🚀 INICIANDO BUSCA DE DADOS DA API PARA ${contactData.id}`);
    
    // Busca dados completos da API Digisac
    const digisacApiData = await fetchContactFromDigisac(contactData.id);
    
    // Verifica mudanças nos campos principais
    const fieldsChanged = await hasFieldsChanged(contactData);
    
    // Verifica mudanças nos campos personalizados
    const customFieldsChanged = await hasCustomFieldsChanged(contactData.id, digisacApiData);
    
    if (!fieldsChanged && !customFieldsChanged) {
      console.log(`ℹ️ NENHUMA MUDANÇA DETECTADA - Contato ${contactData.id}`);
      return { processed: false, reason: 'no_changes' };
    }
    
    console.log(`✅ MUDANÇAS DETECTADAS - Adicionando à lista de envio: ${contactData.id}`);
    
    // Busca dados completos novamente (para garantir dados atualizados)
    const freshDigisacApiData = await fetchContactFromDigisac(contactData.id);
    
    // Busca tickets do contato
    const contactTickets = await fetchContactTickets(contactData.id);
    
    // Transforma dados para formato do CRM
    const crmPayload = await transformToCrmFormat(contactData, freshDigisacApiData, contactTickets);
    
    // Envia para o CRM
    const result = await sendToCrm(contactData, crmPayload);
    
    return { processed: true, result: result };
    
  } catch (error) {
    console.error(`❌ Erro ao processar contato ${contactData.id}:`, error.message);
    throw error;
  }
}

// Função principal para processar o buffer
async function processBuffer() {
  try {
    console.log('🔄 PROCESSANDO BUFFER APÓS 20 SEGUNDOS...');
    console.log(`📊 Total de registros no buffer: ${dataBuffer.size}`);
    
    if (dataBuffer.size === 0) {
      console.log('📭 Buffer vazio - Nada para processar');
      bufferTimer = null;
      return;
    }
    
    const contactsToProcess = [];
    const contactsWithoutChanges = [];
    
    // Analisa cada contato no buffer
    for (const [contactId, contactData] of dataBuffer) {
      console.log(`🔍 Analisando ID: ${contactId}`);
      
      try {
        const result = await processContact(contactData);
        
        if (result.processed) {
          contactsToProcess.push(contactData);
        } else {
          contactsWithoutChanges.push(contactData);
          console.log(`ℹ️ SEM MUDANÇAS - Ignorando: ${contactId}`);
        }
        
      } catch (error) {
        console.error(`❌ Erro ao analisar contato ${contactId}:`, error.message);
        // Continua processando outros contatos mesmo se um falhar
      }
    }
    
    console.log(`📊 SEM MUDANÇAS: ${contactsWithoutChanges.length} contatos`);
    console.log(`📤 TOTAL PARA PROCESSAR E ENVIAR AO CRM: ${contactsToProcess.length}`);
    
    // Limpa o buffer
    console.log(`🧹 Limpando buffer (${dataBuffer.size} registros removidos)`);
    dataBuffer.clear();
    bufferTimer = null;
    
    console.log('✅ PROCESSAMENTO DO BUFFER CONCLUÍDO');
    
  } catch (error) {
    console.error('❌ Erro crítico no processamento do buffer:', error.message);
    
    // Limpa o buffer mesmo em caso de erro para evitar loop infinito
    dataBuffer.clear();
    bufferTimer = null;
  }
}

// Endpoint para verificar status
app.get('/status', (req, res) => {
  res.json({
    status: 'running',
    bufferSize: dataBuffer.size,
    hasTimer: !!bufferTimer,
    timestamp: new Date().toISOString()
  });
});

// Endpoint para forçar processamento do buffer (para testes)
app.post('/force-process', async (req, res) => {
  try {
    if (bufferTimer) {
      clearTimeout(bufferTimer);
      bufferTimer = null;
    }
    
    await processBuffer();
    
    res.json({
      status: 'success',
      message: 'Buffer processado com sucesso',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Inicia o servidor
app.listen(CONFIG.port, () => {
  console.log(`🚀 Servidor rodando na porta ${CONFIG.port}`);
  console.log(`📡 Webhook endpoint: http://localhost:${CONFIG.port}/webhook`);
  console.log(`📊 Status endpoint: http://localhost:${CONFIG.port}/status`);
  console.log(`🔧 Force process endpoint: http://localhost:${CONFIG.port}/force-process`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('🛑 Recebido SIGINT, fechando servidor...');
  
  if (bufferTimer) {
    clearTimeout(bufferTimer);
  }
  
  db.close((err) => {
    if (err) {
      console.error('❌ Erro ao fechar banco de dados:', err.message);
    } else {
      console.log('✅ Banco de dados fechado');
    }
    process.exit(0);
  });
});

