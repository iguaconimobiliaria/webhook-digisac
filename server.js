const express = require('express');
const axios = require('axios');
const fs = require('fs');
const _ = require('lodash');
const sqlite3 = require('sqlite3').verbose();

const app = express();
app.use(express.json({ limit: '10mb' }));

// Configurações - Porta dinâmica para Render
const CONFIG = {
  makeWebhookUrl: 'https://hook.us2.make.com/low1xo1nc7wgk45253bpqzsbe63pman8',
  port: process.env.PORT || 3000, // Render usa PORT environment variable
  bufferTime: 20000, // 20 segundos de acumulação
  maxRetries: 3,
  retryDelay: 2000,
  requestTimeout: 10000
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

    console.log('✅ Banco de dados inicializado');
  });
}

// Buffer para acumular dados por 20 segundos
let dataBuffer = new Map();
let bufferTimer = null;

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
    
    console.log(`📤 TOTAL PARA ENVIAR AO MAKE.COM: ${contactsToSend.length}`);
    
    // Envia para Make.com
    for (const contactData of contactsToSend) {
      const payload = {
        id: contactData.id,
        name: contactData.name,
        number: contactData.number,
        note: contactData.note,
        timestamp: contactData.timestamp,
        event: contactData.originalData.event || 'contact.updated'
      };
      
      const success = await sendToMakeWithRetry(contactData.id, payload);
      
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

async function sendToMakeWithRetry(contactId, payload, attempt = 1) {
  try {
    console.log(`📤 Tentativa ${attempt} - Enviando ${contactId} para Make.com`);
    console.log('📦 PAYLOAD:', JSON.stringify(payload, null, 2));
    
    const response = await axios.post(CONFIG.makeWebhookUrl, payload, {
      timeout: CONFIG.requestTimeout,
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'Webhook-Processor/1.0'
      }
    });

    await logSendAttempt(contactId, payload, 'success', response.data, response.status);
    
    console.log(`✅ Enviado com sucesso para Make.com:`, {
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
      return sendToMakeWithRetry(contactId, payload, attempt + 1);
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
    status: 'Webhook Processor Online',
    message: 'Sistema funcionando corretamente',
    endpoints: {
      webhook: '/webhook',
      status: '/status',
      buffer: '/buffer-info'
    }
  });
});

app.get('/status', (req, res) => {
  res.json({
    status: 'running',
    buffer_size: dataBuffer.size,
    buffer_timer_active: bufferTimer !== null,
    config: {
      buffer_time: CONFIG.bufferTime,
      webhook_url: CONFIG.makeWebhookUrl.substring(0, 50) + '...',
      max_retries: CONFIG.maxRetries
    }
  });
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
  console.log(`🚀 SERVIDOR rodando na porta ${CONFIG.port}`);
  console.log(`⏰ Buffer de acumulação: ${CONFIG.bufferTime/1000} segundos`);
  console.log(`🔗 Make.com URL: ${CONFIG.makeWebhookUrl.substring(0, 50)}...`);
  console.log(`🎯 MONITORANDO MUDANÇAS EM: name, note, number`);
  console.log(`📋 LÓGICA: Acumula 20s → Analisa por ID → Envia apenas alterados`);
});

