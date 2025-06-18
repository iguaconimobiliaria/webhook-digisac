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

// Mapeamentos de IDs
const USER_ID_MAPPING = {
  '64a82223-f414-4ad2-9275-eb20154de6dc': 87, // Cleusa
  'ec2b04ae-939b-4da5-90e0-f3702a007f5d': 1,  // Admin
  '48b2180f-55d2-4b4d-a9b7-9b583c7ac599': 77, // Suelin
  'ee5fa9f0-e203-4138-b0e1-48c6d23d3d3c': 49, // Lucas
  '9802485b-9f3e-45a9-91fa-891e37918e3d': 12  // Lucia
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
    console.log(`🔍 Buscando dados completos do contato ${contactId} na API Digisac`);
    
    const url = `${CONFIG.digisacApiUrl}/contacts/${contactId}?include[0]=customFieldValues&include[1]=tags&include[2]=tickets`;
    
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${CONFIG.digisacToken}`,
        'Content-Type': 'application/json'
      },
      timeout: CONFIG.requestTimeout
    });

    console.log(`✅ Dados do contato ${contactId} obtidos com sucesso`);
    
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
    console.error(`❌ Erro ao buscar contato ${contactId} na API Digisac:`, error.message);
    if (error.response) {
      console.error('Response status:', error.response.status);
      console.error('Response data:', error.response.data);
    }
    throw error;
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

// Função para extrair user ID dos tickets
function extractUserIdFromTickets(tickets) {
  if (!tickets || !Array.isArray(tickets) || tickets.length === 0) {
    return 1; // Admin como padrão
  }
  
  // Pega o ticket mais recente
  const latestTicket = tickets.sort((a, b) => 
    new Date(b.createdAt) - new Date(a.createdAt)
  )[0];
  
  const digisacUserId = latestTicket.userId;
  return USER_ID_MAPPING[digisacUserId] || 1; // Admin como fallback
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
  
  // Verifica se é número brasileiro (código 55)
  if (cleanNumber.startsWith('55') && cleanNumber.length >= 12) {
    const brazilianNumber = cleanNumber.substring(2); // Remove o 55
    
    console.log(`📱 NÚMERO BRASILEIRO:`, {
      semCodigo: brazilianNumber,
      tamanho: brazilianNumber.length
    });
    
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
async function transformToCrmFormat(contactData, digisacApiData) {
  try {
    const { cellNumber, phoneNumber, internationalPhoneNumber } = formatPhoneNumber(contactData.number);
    const email = extractEmailFromCustomFields(digisacApiData.customFieldValues);
    const source = extractSourceFromTags(digisacApiData.tags);
    const userId = extractUserIdFromTickets(digisacApiData.tickets);
    
    // Usar o ID real do contato da Digisac
    const contactId = contactData.id;
    
    // Extrair dados reais do usuário/atendente dos tickets
    let userData = {
      id: userId,
      username: "elliot", // Padrão se não encontrar
      email: "elliot@email.com", // Padrão se não encontrar  
      name: "Elliot Alderson" // Padrão se não encontrar
    };
    
    // Se tem tickets, pega dados do usuário do ticket mais recente
    if (digisacApiData.tickets && digisacApiData.tickets.length > 0) {
      const latestTicket = digisacApiData.tickets.sort((a, b) => 
        new Date(b.createdAt) - new Date(a.createdAt)
      )[0];
      
      if (latestTicket.user) {
        userData = {
          id: USER_ID_MAPPING[latestTicket.userId] || userId,
          username: latestTicket.user.username || "elliot",
          email: latestTicket.user.email || "elliot@email.com",
          name: latestTicket.user.name || "Elliot Alderson"
        };
      }
    }
    
    const crmPayload = {
      // ID removido - CRM vai gerar automaticamente
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
        id: userId, // ID mapeado do atendente Digisac → CRM
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
    
    console.log(`🔄 Dados transformados para CRM:`, {
      leadId: 'AUTO_GENERATED', // CRM vai gerar automaticamente
      contactId: contactData.id,
      name: crmPayload.name,
      source: crmPayload.source,
      email: crmPayload.email,
      userId: crmPayload.user.id, // ID mapeado do atendente
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
        
        // 2. Transforma para formato do CRM
        const crmPayload = await transformToCrmFormat(contactData, digisacApiData);
        
        // 3. GERA TOKEN APENAS AGORA (depois de verificar mudanças e formatar dados)
        console.log('🔑 Gerando token CRM apenas agora...');
        const crmToken = await getCrmToken();
        
        // 4. Envia para CRM com token gerado
        const success = await sendToCrmDirect(contactData.id, crmPayload, crmToken);
        
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
        await logSendAttempt(contactData.id, {}, 'error', error.message, 'PROCESSING_ERROR');
      }
    }
    
    // Limpa o buffer após processamento
    console.log(`🧹 Limpando buffer (${dataBuffer.size} registros removidos)`);
    dataBuffer.clear();
    bufferTimer = null;
    
    console.log('✅ PROCESSAMENTO DO BUFFER CONCLUÍDO');
    
  } catch (error) {
    console.error('❌ Erro no processamento do buffer:', error.message);
    bufferTimer = null;
  }
}

// Função para comparar campos específicos
function hasFieldsChanged(current, previous) {
  if (!previous) {
    console.log(`🆕 Primeiro registro para ID ${current.id} - enviando`);
    return true;
  }
  
  const currentName = current.name || '';
  const currentNote = current.note || '';
  const currentNumber = current.number || '';
  
  const previousName = previous.name || '';
  const previousNote = previous.note || '';
  const previousNumber = previous.number || '';
  
  const nameChanged = currentName !== previousName;
  const noteChanged = currentNote !== previousNote;
  const numberChanged = currentNumber !== previousNumber;
  
  console.log(`🔍 COMPARAÇÃO PARA ${current.id}:`, {
    name: { atual: currentName, anterior: previousName, mudou: nameChanged },
    note: { atual: currentNote, anterior: previousNote, mudou: noteChanged },
    number: { atual: currentNumber, anterior: previousNumber, mudou: numberChanged }
  });
  
  return nameChanged || noteChanged || numberChanged;
}

async function sendToCrmDirect(contactId, payload, crmToken) {
  try {
    console.log(`📤 Enviando ${contactId} para CRM com token gerado`);
    
    console.log('📦 PAYLOAD CRM:', JSON.stringify(payload, null, 2));
    
    const response = await axios.post(CONFIG.crmApiUrl, payload, {
      timeout: CONFIG.requestTimeout,
      headers: {
        'Authorization': `Bearer ${crmToken}`,
        'Content-Type': 'application/json',
        'User-Agent': 'Digisac-CRM-Integration/1.0'
      }
    });

    await logSendAttempt(contactId, payload, 'success', response.data, response.status);
    
    console.log(`✅ Enviado com sucesso para CRM:`, {
      contactId,
      status: response.status,
      data: response.data
    });
    
    return true;
    
  } catch (error) {
    const errorMessage = error.response?.data || error.message;
    const statusCode = error.response?.status || 'NETWORK_ERROR';
    
    console.error(`❌ Erro ao enviar ${contactId} para CRM:`, {
      status: statusCode,
      message: errorMessage,
      url: CONFIG.crmApiUrl
    });

    await logSendAttempt(contactId, payload, 'error', errorMessage, statusCode);
    return false;
  }
}

async function sendToCrmWithRetry(contactId, payload, attempt = 1) {
  try {
    console.log(`📤 Tentativa ${attempt} - Enviando ${contactId} para CRM`);
    
    // Gera token do CRM
    const crmToken = await getCrmToken();
    
    console.log('📦 PAYLOAD CRM:', JSON.stringify(payload, null, 2));
    
    const response = await axios.post(CONFIG.crmApiUrl, payload, {
      timeout: CONFIG.requestTimeout,
      headers: {
        'Authorization': `Bearer ${crmToken}`,
        'Content-Type': 'application/json',
        'User-Agent': 'Digisac-CRM-Integration/1.0'
      }
    });

    await logSendAttempt(contactId, payload, 'success', response.data, response.status);
    
    console.log(`✅ Enviado com sucesso para CRM:`, {
      contactId,
      status: response.status
    });
    
    return true;
    
  } catch (error) {
    const errorMessage = error.response?.data || error.message;
    const statusCode = error.response?.status || 'NETWORK_ERROR';
    
    console.error(`❌ Erro na tentativa ${attempt} para ${contactId}:`, {
      status: statusCode,
      message: errorMessage
    });

    await logSendAttempt(contactId, payload, 'error', errorMessage, statusCode);

    if (attempt < CONFIG.maxRetries) {
      console.log(`🔄 Aguardando ${CONFIG.retryDelay}ms antes da próxima tentativa...`);
      await new Promise(resolve => setTimeout(resolve, CONFIG.retryDelay));
      return sendToCrmWithRetry(contactId, payload, attempt + 1);
    } else {
      console.error(`💥 Falha definitiva após ${CONFIG.maxRetries} tentativas para ${contactId}`);
      return false;
    }
  }
}

async function logSendAttempt(contactId, payload, status, response, statusCode) {
  try {
    await new Promise((resolve, reject) => {
      db.run(
        'INSERT INTO send_logs (contact_id, payload, status, response, timestamp) VALUES (?, ?, ?, ?, ?)',
        [contactId, JSON.stringify(payload), `${status}_${statusCode}`, JSON.stringify(response), new Date().toISOString()],
        function(err) {
          if (err) reject(err);
          else resolve(this.lastID);
        }
      );
    });
  } catch (error) {
    console.error('❌ Erro ao salvar log de envio:', error.message);
  }
}

// Endpoints de debug
app.get('/', (req, res) => {
  res.json({
    status: 'Digisac-CRM Integration Online',
    message: 'Sistema funcionando corretamente',
    endpoints: {
      webhook: '/webhook',
      status: '/status',
      buffer: '/buffer-info',
      logs: '/logs',
      'test-crm-token': '/test-crm-token'
    }
  });
});

app.get('/status', (req, res) => {
  res.json({
    status: 'running',
    buffer_size: dataBuffer.size,
    buffer_timer_active: bufferTimer !== null,
    crm_token_cached: crmTokenCache.token !== null,
    crm_token_expires: crmTokenCache.expiresAt,
    config: {
      buffer_time: CONFIG.bufferTime,
      digisac_api: CONFIG.digisacApiUrl,
      crm_api: CONFIG.crmApiUrl,
      max_retries: CONFIG.maxRetries
    }
  });
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
      message: error.message 
    });
  }
});

app.get('/logs', async (req, res) => {
  try {
    const logs = await new Promise((resolve, reject) => {
      db.all(
        'SELECT * FROM send_logs ORDER BY timestamp DESC LIMIT 50',
        [],
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      );
    });
    
    res.json({ logs });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/force-process', async (req, res) => {
  try {
    if (bufferTimer) {
      clearTimeout(bufferTimer);
      bufferTimer = null;
    }
    await processBuffer();
    res.json({ status: 'success', message: 'Buffer processado manualmente' });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/buffer-info', (req, res) => {
  const bufferData = Array.from(dataBuffer.entries()).map(([id, data]) => ({
    id,
    name: data.name,
    note: data.note,
    number: data.number,
    timestamp: data.timestamp
  }));
  
  res.json({
    buffer_size: dataBuffer.size,
    timer_active: bufferTimer !== null,
    data: bufferData
  });
});

process.on('SIGINT', () => {
  console.log('🛑 Encerrando servidor...');
  if (bufferTimer) {
    clearTimeout(bufferTimer);
  }
  db.close((err) => {
    if (err) console.error('Erro ao fechar banco:', err.message);
    else console.log('✅ Banco fechado com sucesso');
    process.exit(0);
  });
});

app.listen(CONFIG.port, () => {
  console.log(`🚀 SERVIDOR DIGISAC-CRM INTEGRATION rodando na porta ${CONFIG.port}`);
  console.log(`⏰ Buffer de acumulação: ${CONFIG.bufferTime/1000} segundos`);
  console.log(`🔗 Digisac API: ${CONFIG.digisacApiUrl}`);
  console.log(`🎯 CRM API: ${CONFIG.crmApiUrl}`);
  console.log(`🔑 Token Digisac: ${CONFIG.digisacToken.substring(0, 10)}...`);
  console.log(`🎯 MONITORANDO MUDANÇAS EM: name, note, number`);
  console.log(`📋 FLUXO: Digisac → Buffer → API Digisac → Transformação → CRM`);
  console.log(`✅ SISTEMA PRONTO PARA PRODUÇÃO!`);
});

