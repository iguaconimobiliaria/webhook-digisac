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
  crmApiUrl: 'https://api.si9sistemas.com.br/imobilsi9-api/lead', // URL corrigida conforme especificação
  crmAuthHeader: 'Basic OTBlYTU4MjctZDQ2Zi00OGE1LTg1NjMtNzQ2YTlmMjBlZDZiOmEwZmU0MDhjLTlhNTQtNDRmMC1iN2I2LTBiMTk0Y2FhNjJlNQ==',
  port: process.env.PORT || 3000,
  bufferTime: 20000, // 20 segundos de acumulação
  maxRetries: 3,
  retryDelay: 2000,
  requestTimeout: 15000
};

// Mapeamentos de IDs - ATUALIZADO COM KLEO
const USER_ID_MAPPING = {
  '64a82223-f414-4ad2-9275-eb20154de6dc': 87, // Cleusa
  'ec2b04ae-939b-4da5-90e0-f3702a007f5d': 1,  // Admin
  '48b2180f-55d2-4b4d-a9b7-9b583c7ac599': 77, // Suelin
  'ee5fa9f0-e203-4138-b0e1-48c6d23d3d3c': 49, // Lucas
  '9802485b-9f3e-45a9-91fa-891e37918e3d': 12, // Lucia
  'e97e9a59-72fb-4b10-a1e9-13eee1fbb1ea': 27  // Kleo
};

const TAG_SOURCE_MAPPING = {
  '9bf06544-6e4b-4d96-903d-2c26e52ca0c1': 'Campanha Jd. Alice - IM. Terceiros',
  '11221ef6-8afc-4ab2-9414-d8fa7fac573a': 'Campanha Ilha Bella - IM. Terceiros',
  'c2fed5c7-92d8-43e4-b255-4d585178bd3e': 'Campanha Vila Maria - IM. Terceiros',
  'e71536ca-9f9c-4c5d-aa79-dfbee22ff332': 'Digisac - Imóveis de Terceiro'
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
    // Tabela para dados enviados (para comparação futura)
    db.run(`CREATE TABLE IF NOT EXISTS sent_data (
      id TEXT PRIMARY KEY,
      name TEXT,
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
    return 'sememail@email.com';
  }
  
  const emailField = customFieldValues.find(field => 
    field.customFieldId === EMAIL_CUSTOM_FIELD_ID
  );
  
  return emailField?.value || 'sememail@email.com';
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

// Função para extrair user ID dos tickets - ATUALIZADA COM KLEO
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
  
  console.log('📋 MAPEAMENTO DISPONÍVEL (INCLUINDO KLEO):', USER_ID_MAPPING);
  
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
  const limitedNumber = cleanNumber.substring(0, 15);
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

// Função para transformar dados para formato do CRM
async function transformToCrmFormat(contactData, digisacApiData, contactTickets) {
  try {
    const { cellNumber, phoneNumber, internationalPhoneNumber } = formatPhoneNumber(contactData.number);
    const email = extractEmailFromCustomFields(digisacApiData.customFieldValues);
    const source = extractSourceFromTags(digisacApiData.tags);
    const userId = extractUserIdFromTickets(contactTickets); // Agora usa tickets separados
    
    const crmPayload = {
      id: 1, // ID fixo conforme solicitado
      name: contactData.name,
      classification: "high",
      interestedIn: "buy",
      source: source,
      cellNumber: cellNumber,
      phoneNumber: phoneNumber,
      internationalPhoneNumber: internationalPhoneNumber,
      email: email,
      observation: contactData.note,
      observationLead: contactData.note,
      user: {
        id: userId, // ID mapeado do atendente Digisac → CRM (INCLUINDO KLEO)
        username: "elliot", // Fixo
        email: "elliot@email.com", // Fixo
        name: "Elliot Alderson" // Fixo
      },
      contacts: [
        {
          propertyId: 123,
          observation: "Lead processado automaticamente via Digisac",
          contactType: 11,
          date: new Date().toISOString().substring(0, 10) + ':' + new Date().toISOString().substring(11, 19)
        }
      ]
    };
    
    console.log(`🔄 Dados transformados para CRM (COM KLEO):`, {
      leadId: 1, // ID fixo do lead
      contactId: contactData.id,
      name: crmPayload.name,
      source: crmPayload.source,
      email: crmPayload.email,
      userId: crmPayload.user.id, // ID mapeado do atendente (pode ser Kleo: 27)
      userName: crmPayload.user.name
    });
    
    return crmPayload;
    
  } catch (error) {
    console.error(`❌ Erro ao transformar dados do contato ${contactData.id}:`, error.message);
    throw error;
  }
}

// Função principal que processa o buffer após 20 segundos
async function processBuffer() {
  console.log('🔄 PROCESSANDO BUFFER APÓS 20 SEGUNDOS...');
  console.log(`📊 Total de registros no buffer: ${dataBuffer.size}`);
  
  if (dataBuffer.size === 0) {
    console.log('ℹ️ Buffer vazio, nada para processar');
    bufferTimer = null;
    return;
  }
  
  try {
    const contactsToSend = [];
    
    // Para cada ID no buffer, verifica se precisa enviar
    for (const [contactId, currentData] of dataBuffer.entries()) {
      console.log(`🔍 Analisando ID: ${contactId}`);
      
      // Busca o último dado enviado deste ID
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
      
      // Verifica se houve mudanças nos campos monitorados
      if (hasFieldsChanged(currentData, lastSentData)) {
        contactsToSend.push(currentData);
        console.log(`✅ ADICIONADO PARA ENVIO: ${contactId}`);
      } else {
        console.log(`⏭️ SEM MUDANÇAS: ${contactId}`);
      }
    }
    
    console.log(`📤 TOTAL PARA PROCESSAR E ENVIAR AO CRM: ${contactsToSend.length}`);
    
    // Processa e envia para CRM
    for (const contactData of contactsToSend) {
      try {
        // 1. Busca dados completos na API Digisac
        const digisacApiData = await fetchContactFromDigisac(contactData.id);
        
        // 2. Busca tickets do contato
        const contactTickets = await fetchContactTickets(contactData.id);
        
        // 3. Transforma para formato do CRM
        const crmPayload = await transformToCrmFormat(contactData, digisacApiData, contactTickets);
        
        // 4. Envia para CRM
        const success = await sendToCrmWithRetry(contactData.id, crmPayload);
        
        if (success) {
          // Salva no banco como "enviado" para próximas comparações
          await new Promise((resolve, reject) => {
            db.run(
              `INSERT OR REPLACE INTO sent_data 
               (id, name, number, note, timestamp) 
               VALUES (?, ?, ?, ?, ?)`,
              [contactData.id, contactData.name, contactData.number, contactData.note, contactData.timestamp],
              function(err) {
                if (err) reject(err);
                else resolve(this.changes);
              }
            );
          });
          
          console.log(`💾 Dados salvos como enviados: ${contactData.id}`);
        }
        
      } catch (error) {
        console.error(`❌ Erro ao processar contato ${contactData.id}:`, error.message);
      }
    }
    
  } catch (error) {
    console.error('❌ Erro no processamento do buffer:', error.message);
  } finally {
    // Limpa o buffer e reseta o timer
    console.log(`🧹 Limpando buffer (${dataBuffer.size} registros removidos)`);
    dataBuffer.clear();
    bufferTimer = null;
    console.log('✅ PROCESSAMENTO DO BUFFER CONCLUÍDO');
  }
}

// Função para verificar se houve mudanças nos campos monitorados
function hasFieldsChanged(currentData, lastSentData) {
  if (!lastSentData) {
    console.log(`🆕 Primeiro registro para ID ${currentData.id} - enviando`);
    return true;
  }
  
  const fieldsToCompare = ['name', 'note', 'number'];
  
  console.log(`🔍 COMPARAÇÃO DE CAMPOS para ID ${currentData.id}:`);
  
  for (const field of fieldsToCompare) {
    const currentValue = currentData[field] || '';
    const lastValue = lastSentData[field] || '';
    
    console.log(`  📋 ${field.toUpperCase()}:`);
    console.log(`    Atual: "${currentValue}"`);
    console.log(`    Último: "${lastValue}"`);
    console.log(`    Mudou: ${currentValue !== lastValue ? '✅ SIM' : '❌ NÃO'}`);
    
    if (currentValue !== lastValue) {
      console.log(`✅ MUDANÇA DETECTADA no campo ${field.toUpperCase()}`);
      return true;
    }
  }
  
  console.log(`⏭️ NENHUMA MUDANÇA nos campos monitorados`);
  return false;
}

// Função para enviar dados para CRM com retry
async function sendToCrmWithRetry(contactId, payload, attempt = 1) {
  try {
    console.log(`📤 [TENTATIVA ${attempt}] Enviando contato ${contactId} para CRM`);
    
    // Gera token do CRM
    const token = await getCrmToken();
    
    console.log(`🔗 [ENVIO] URL: ${CONFIG.crmApiUrl}`);
    console.log(`🔑 [ENVIO] Token: ${token ? 'PRESENTE' : 'AUSENTE'}`);
    console.log(`📦 [ENVIO] Payload:`, JSON.stringify(payload, null, 2));
    
    const response = await axios.post(CONFIG.crmApiUrl, payload, {
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      timeout: CONFIG.requestTimeout
    });
    
    console.log(`✅ [SUCESSO] Contato ${contactId} enviado para CRM`);
    console.log(`📊 [SUCESSO] Status: ${response.status}`);
    console.log(`📊 [SUCESSO] Response:`, response.data);
    
    // Log no banco
    await new Promise((resolve, reject) => {
      db.run(
        'INSERT INTO send_logs (contact_id, payload, status, response) VALUES (?, ?, ?, ?)',
        [contactId, JSON.stringify(payload), 'success', JSON.stringify(response.data)],
        function(err) {
          if (err) reject(err);
          else resolve(this.changes);
        }
      );
    });
    
    return true;
    
  } catch (error) {
    console.error(`❌ [ERRO TENTATIVA ${attempt}] Falha ao enviar contato ${contactId} para CRM:`);
    console.error(`📊 [ERRO] Tipo:`, error.constructor.name);
    console.error(`📊 [ERRO] Mensagem:`, error.message);
    
    if (error.response) {
      console.error(`📊 [ERRO] Status:`, error.response.status);
      console.error(`📊 [ERRO] Data:`, error.response.data);
    }
    
    // Log do erro no banco
    await new Promise((resolve, reject) => {
      db.run(
        'INSERT INTO send_logs (contact_id, payload, status, response) VALUES (?, ?, ?, ?)',
        [contactId, JSON.stringify(payload), 'error', error.message],
        function(err) {
          if (err) reject(err);
          else resolve(this.changes);
        }
      );
    });
    
    // Retry se não excedeu o limite
    if (attempt < CONFIG.maxRetries) {
      console.log(`🔄 [RETRY] Tentando novamente em ${CONFIG.retryDelay}ms...`);
      await new Promise(resolve => setTimeout(resolve, CONFIG.retryDelay));
      return sendToCrmWithRetry(contactId, payload, attempt + 1);
    }
    
    console.error(`❌ [FALHA FINAL] Todas as ${CONFIG.maxRetries} tentativas falharam para ${contactId}`);
    return false;
  }
}

// Endpoints de utilidade
app.get('/', (req, res) => {
  res.json({ 
    status: 'online', 
    message: 'Webhook Digisac-CRM Integration - ATUALIZADO COM KLEO',
    version: '2.0.0',
    timestamp: new Date().toISOString(),
    userMapping: Object.keys(USER_ID_MAPPING).length + ' atendentes mapeados (incluindo Kleo)'
  });
});

app.get('/status', (req, res) => {
  res.json({
    status: 'running',
    buffer_size: dataBuffer.size,
    buffer_timer_active: !!bufferTimer,
    crm_token_cached: !!crmTokenCache.token,
    crm_token_expires: crmTokenCache.expiresAt,
    user_mapping_count: Object.keys(USER_ID_MAPPING).length,
    kleo_included: USER_ID_MAPPING['e97e9a59-72fb-4b10-a1e9-13eee1fbb1ea'] === 27,
    config: {
      buffer_time: CONFIG.bufferTime,
      digisac_api: CONFIG.digisacApiUrl,
      crm_api: CONFIG.crmApiUrl
    }
  });
});

app.get('/buffer-info', (req, res) => {
  const bufferArray = Array.from(dataBuffer.entries()).map(([id, data]) => ({
    id,
    name: data.name,
    number: data.number,
    note: data.note,
    timestamp: data.timestamp
  }));
  
  res.json({
    buffer_size: dataBuffer.size,
    timer_active: !!bufferTimer,
    contacts: bufferArray
  });
});

app.post('/force-process', async (req, res) => {
  if (bufferTimer) {
    clearTimeout(bufferTimer);
    bufferTimer = null;
  }
  
  res.json({ message: 'Processamento forçado iniciado' });
  
  try {
    await processBuffer();
  } catch (error) {
    console.error('❌ Erro no processamento forçado:', error.message);
  }
});

app.post('/clear-database', (req, res) => {
  db.serialize(() => {
    db.run('DELETE FROM sent_data');
    db.run('DELETE FROM send_logs');
    db.run('DELETE FROM digisac_cache');
  });
  
  res.json({ message: 'Banco de dados limpo com sucesso' });
});

app.get('/test-crm-token', async (req, res) => {
  try {
    const token = await getCrmToken();
    res.json({ 
      status: 'success', 
      message: 'Token CRM gerado com sucesso',
      token_preview: token.substring(0, 20) + '...',
      expires_at: crmTokenCache.expiresAt
    });
  } catch (error) {
    res.status(500).json({ 
      status: 'error', 
      message: 'Erro ao gerar token CRM',
      error: error.message 
    });
  }
});

// Endpoint para testar mapeamento de usuários (incluindo Kleo)
app.get('/test-user-mapping', (req, res) => {
  res.json({
    message: 'Mapeamento de usuários atualizado com Kleo',
    total_users: Object.keys(USER_ID_MAPPING).length,
    users: {
      'Cleusa': { digisac: '64a82223-f414-4ad2-9275-eb20154de6dc', crm: 87 },
      'Admin': { digisac: 'ec2b04ae-939b-4da5-90e0-f3702a007f5d', crm: 1 },
      'Suelin': { digisac: '48b2180f-55d2-4b4d-a9b7-9b583c7ac599', crm: 77 },
      'Lucas': { digisac: 'ee5fa9f0-e203-4138-b0e1-48c6d23d3d3c', crm: 49 },
      'Lucia': { digisac: '9802485b-9f3e-45a9-91fa-891e37918e3d', crm: 12 },
      'Kleo': { digisac: 'e97e9a59-72fb-4b10-a1e9-13eee1fbb1ea', crm: 27 }
    },
    mapping: USER_ID_MAPPING
  });
});

// Inicialização do servidor
const server = app.listen(CONFIG.port, '0.0.0.0', () => {
  console.log('🚀 SERVIDOR DIGISAC-CRM INTEGRATION rodando na porta', CONFIG.port);
  console.log('⏰ Buffer de acumulação:', CONFIG.bufferTime / 1000, 'segundos');
  console.log('🔗 Digisac API:', CONFIG.digisacApiUrl);
  console.log('🎯 CRM API:', CONFIG.crmApiUrl);
  console.log('🔑 Token Digisac:', CONFIG.digisacToken ? CONFIG.digisacToken.substring(0, 10) + '...' : 'NÃO CONFIGURADO');
  console.log('👥 Atendentes mapeados:', Object.keys(USER_ID_MAPPING).length, '(incluindo Kleo)');
  console.log('✅ SISTEMA PRONTO PARA PRODUÇÃO COM KLEO!');
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('🛑 Recebido SIGTERM, encerrando servidor...');
  server.close(() => {
    console.log('✅ Servidor encerrado');
    db.close();
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('🛑 Recebido SIGINT, encerrando servidor...');
  server.close(() => {
    console.log('✅ Servidor encerrado');
    db.close();
    process.exit(0);
  });
});

