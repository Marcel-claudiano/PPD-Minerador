import paho.mqtt.client as mqtt
import json
import random
import time
import sys
import threading
import hashlib

# Configurações globais
BROKER = "broker.emqx.io"
PORT = 1883

# Topicos do MQTT
TOPIC_INIT = "sd/init"
TOPIC_VOTE = "sd/voting"
TOPIC_CHALLENGE = "sd/challenge"
TOPIC_SOLUTION = "sd/solution"
TOPIC_RESULT = "sd/result"

class Minerador:
    def __init__(self, num_participantes):
        self.n = int(num_participantes)
        self.id = random.randint(0, 65535)
        self.vote_val = random.randint(0, 65535)
        
        self.estado = "INIT"
        self.lider = None
        self.sou_lider = False
        
        self.lista_inits = set()
        self.votos = {}
        
        self.transacao_atual = 0
        self.desafio_atual = 0
        self.minerando = False
        
        self.transacao_encerrada = False
        
        self.client = mqtt.Client(client_id=str(self.id), protocol=mqtt.MQTTv311)
        self.client.on_connect = self.conectar
        self.client.on_message = self.mensagem

    def conectar(self, client, userdata, flags, rc):
        print(f"[{self.id}] Conectado no broker!")
        client.subscribe([(TOPIC_INIT, 0), (TOPIC_VOTE, 0), 
                          (TOPIC_CHALLENGE, 0), (TOPIC_SOLUTION, 0), (TOPIC_RESULT, 0)])
        
        t = threading.Thread(target=self.loop_init)
        t.daemon = True
        t.start()

    def loop_init(self):
        # Continua mandando INIT até a eleição estar avançada (para destravar quem chegar tarde)
        while self.estado == "INIT" or (self.estado == "ELECTION" and len(self.votos) < self.n):
            try:
                msg = {"ClientID": self.id}
                self.client.publish(TOPIC_INIT, json.dumps(msg))
                time.sleep(1.5)
            except:
                pass

    def mensagem(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            topico = msg.topic
            
            # --- Fase 1: Inicialização ---
            if topico == TOPIC_INIT:
                if self.estado != "INIT": return
                
                remetente = payload["ClientID"]
                if remetente not in self.lista_inits:
                    self.lista_inits.add(remetente)
                    print(f"[{self.id}] Achei o cliente {remetente}. Total: {len(self.lista_inits)}/{self.n}")
                
                if len(self.lista_inits) >= self.n:
                    print(f"[{self.id}] Todo mundo conectado. Indo para eleição...")
                    self.estado = "ELECTION"
                    time.sleep(1)
                    self.enviar_voto()

            # --- Fase 2: Eleição (Aceita votos mesmo se chegar atrasado) ---
            elif topico == TOPIC_VOTE:
                cid = payload["ClientID"]
                voto = payload["VoteID"]
                
                if cid not in self.votos:
                    self.votos[cid] = voto
                    if self.estado == "ELECTION":
                        print(f"[{self.id}] Recebi voto de {cid}: {voto}")
                
                if self.estado == "ELECTION" and len(self.votos) >= self.n:
                    self.definir_lider()

            # --- Fase 3: Desafio ---
            elif topico == TOPIC_CHALLENGE:
                self.estado = "RUNNING"
                tid = payload["TransactionID"]
                dif = payload["Challenge"]
                
                print(f"\n[{self.id}] DESAFIO NOVO! ID: {tid} | Zeros: {dif}")
                self.transacao_atual = tid
                self.desafio_atual = dif
                self.minerando = True
                self.transacao_encerrada = False # Reseta para nova rodada
                
                t_min = threading.Thread(target=self.minerar)
                t_min.start()

            # --- Fase 4: Solução ---
            elif topico == TOPIC_SOLUTION and self.sou_lider:
                # Se eu já encerrei essa transação, ignoro soluções atrasadas
                if self.transacao_encerrada:
                    return

                print(f"[{self.id}] Verificando solução de {payload['ClientID']}...")
                sol = payload['Solution']
                
                # Verifica se a solução bate com o desafio atual
                if str(payload['TransactionID']) == str(self.transacao_atual):
                    if self.validar_hash(sol, self.desafio_atual):
                        
                        # TRAVA: Marca que acabou para não aceitar outro
                        self.transacao_encerrada = True
                        
                        print(f"[{self.id}] Solução correta! O vencedor é {payload['ClientID']}")
                        resp = {
                            "ClientID": payload['ClientID'],
                            "TransactionID": payload['TransactionID'],
                            "Solution": sol,
                            "Result": 1
                        }
                        self.client.publish(TOPIC_RESULT, json.dumps(resp))
                        
                        # Delay de 5s para dar tempo de lerem
                        threading.Timer(5, self.gerar_desafio).start()

            # --- Resultado ---
            elif topico == TOPIC_RESULT and payload['Result'] == 1:
                # Só imprime se for a primeira vez que recebe o aviso (evita spam)
                if self.minerando:
                    print(f"[{self.id}] O desafio acabou. Vencedor: {payload['ClientID']}")
                    self.minerando = False

        except Exception as e:
            pass

    def enviar_voto(self):
        msg = {"ClientID": self.id, "VoteID": self.vote_val}
        self.client.publish(TOPIC_VOTE, json.dumps(msg))
    
    def definir_lider(self):
        if self.lider is not None: return

        maior_voto = -1
        ganhador = -1
        
        for cid, voto in self.votos.items():
            if voto > maior_voto:
                maior_voto = voto
                ganhador = cid
            elif voto == maior_voto:
                if cid > ganhador:
                    ganhador = cid
        
        self.lider = ganhador
        print(f"[{self.id}] Lider eleito: {self.lider}")
        
        if self.lider == self.id:
            print(f"[{self.id}] EU SOU O LIDER!")
            self.sou_lider = True
            self.estado = "CHALLENGE"
            time.sleep(2)
            self.gerar_desafio()
        else:
            self.sou_lider = False
            self.estado = "RUNNING"

    def gerar_desafio(self):
        if not self.sou_lider: return
        
        self.transacao_atual += 1
        self.desafio_atual = random.randint(1, 20)
        
        msg = {
            "TransactionID": self.transacao_atual,
            "Challenge": self.desafio_atual
        }
        self.client.publish(TOPIC_CHALLENGE, json.dumps(msg))

    def minerar(self):
        target = "0" * self.desafio_atual
        nonce = 0
        print(f"[{self.id}] Começando a minerar...")
        
        # Guarda o ID localmente pra saber quando parar
        tid_local = self.transacao_atual

        while self.minerando and tid_local == self.transacao_atual:
            teste = f"{self.id}-{self.transacao_atual}-{nonce}"
            h = hashlib.sha1(teste.encode()).hexdigest()
            
            if h.startswith(target):
                print(f"[{self.id}] Achei! Hash: {h}")
                msg = {
                    "ClientID": self.id,
                    "TransactionID": self.transacao_atual,
                    "Solution": teste
                }
                self.client.publish(TOPIC_SOLUTION, json.dumps(msg))
                self.minerando = False
                break
            
            nonce += 1
            if nonce % 5000 == 0:
                time.sleep(0.001)

    def validar_hash(self, solucao, zeros):
        h = hashlib.sha1(solucao.encode()).hexdigest()
        if h.startswith("0" * zeros):
            return True
        return False

    def iniciar(self):
        self.client.connect(BROKER, PORT, 60)
        self.client.loop_forever()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        n = sys.argv[1]
    else:
        n = 3 
    
    node = Minerador(n)
    node.iniciar()