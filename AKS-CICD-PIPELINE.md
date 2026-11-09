# Kafka on AKS — Automated Delivery via GitHub → Jenkins → Argo CD → Helm

This document is self-contained so anyone (or any AI assistant) can understand **the current setup**, **where we are going**, and **how the automation works**.

---

## 1. Current state (what already exists and works)

### 1.1 Local Docker (learning, done)
- Local Compose stack used for understanding the flow:
  - `broker` — `confluentinc/cp-kafka:8.0.0` in KRaft mode (single node, broker+controller)
  - `kafka-ui` — `kafbat/kafka-ui:latest`, connected to `broker:29092`
  - Produced/consumed messages with `kafka-console-producer`/`kafka-console-consumer`.
  - Works. Kafka UI is reachable locally (after resolving a Docker Desktop port-forward quirk).

### 1.2 Local Kubernetes (learning, done)
- Cluster: **Docker Desktop Kubernetes** (`docker-desktop` context).
- Installed **Strimzi operator** (namespace `kafka`) via `kubectl create -f https://strimzi.io/install/latest?namespace=kafka`.
- Deployed a **single-node KRaft Kafka** using Strimzi CRDs:
  - `KafkaNodePool` `dual-role` (1 replica, roles: controller+broker)
  - `Kafka` `my-cluster` (Kafka `4.3.2`, internal listener `plain:9092`, replication factors = 1)
  - Manifest: `kafka-local/k8s/kafka-cluster.yaml`
- Created topic via `KafkaTopic` CR:
  - `test-topic` (1 partition, 1 replica)
  - Manifest: `kafka-local/k8s/topic.yaml`
- Verified produce/consume with `kubectl run` console producer/consumer.
- Ran Kafka UI in-cluster (`kafbat/kafka-ui`) and used `kubectl port-forward` to view it.

### 1.3 Existing docs
- `kafka-local/KUBERNETES-LEARNING-GUIDE.md` — K8s/Strimzi concepts + checklist.
- `kafka-local/AKS-DEPLOYMENT.md` — manual AKS steps (az CLI, Helm install of Strimzi, adapt manifests).
- This file — automated pipeline plan + current state.

### 1.4 Repo context
This repo (`new-frontend-albha`) also contains the broader platform assets (frontends, FLINK, KAFKA Compose setups, Keycloak, etc.). For this automation, we focus on the **Kafka + Kafka UI on AKS** slice.

---

## 2. Goal / definition of done

Final outcome on AKS (verified end to end):

1. **Kafka UI is deployed on AKS** and reachable (via Ingress or port-forward for verification).
2. A **producer** can publish messages to a Kafka topic from a **cloud terminal** (`kubectl run` pod or an app).
3. A **consumer** reads those messages (consumer group visible).
4. In **Kafka UI**, you can see:
   - the cluster/broker is online
   - the topic (e.g., `test-topic`) exists
   - the produced messages in **Topics → messages**
   - the consumer group and offsets/lag in **Consumers**

---

## 2.1 Manual AKS deployment first (Milestone 1 — do this before automating)

Before wiring Jenkins/Argo CD, prove the outcome manually. This is the same local flow moved to AKS.

### A. Create AKS

```powershell
$RG="rg-kafka-dev"
$AKS="aks-kafka-dev"
$LOC="centralindia"

az login
az group create --name $RG --location $LOC

az aks create `
  --resource-group $RG `
  --name $AKS `
  --location $LOC `
  --node-count 2 `
  --node-vm-size Standard_D2s_v5 `
  --enable-managed-identity `
  --generate-ssh-keys

az aks get-credentials --resource-group $RG --name $AKS

kubectl config current-context
kubectl get nodes
kubectl get storageclass
```

Note the Azure StorageClass to use for Kafka (commonly `managed-csi`; `managed-csi-premium` for faster disks).

### B. Install Strimzi operator with Helm

```powershell
helm repo add strimzi https://strimzi.io/charts/
helm repo update

kubectl create namespace kafka
helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator -n kafka

kubectl get pods -n kafka -w
# Ctrl+C when strimzi-cluster-operator-* is 1/1 Running

# Which Kafka versions does this operator support? Use one below.
kubectl set env deployment/strimzi-cluster-operator -n kafka --list | Select-String "STRIMZI_KAFKA_IMAGES"
```

### C. Deploy Kafka (adapt the local manifest)

Reuse `kafka-local/k8s/kafka-cluster.yaml`, with AKS-specific changes:

- `spec.kafka.version` = a version supported by the installed Strimzi operator
- storage block:

```yaml
  storage:
    type: jbod
    volumes:
      - id: 0
        type: persistent-claim
        size: 20Gi
        deleteClaim: false
        class: managed-csi
```

Start with `replicas: 1` + replication factors `1` (smoke test), then move to 3 nodes/HA later.

```powershell
kubectl apply -f kafka-local/k8s/kafka-cluster.yaml
kubectl get pods -n kafka -w
kubectl get kafka,kafkanodepool,svc,pvc -n kafka
```

Expected: `my-cluster` `Ready`, service `my-cluster-kafka-bootstrap`, PVCs `Bound`.

### D. Create the topic

```powershell
kubectl apply -f kafka-local/k8s/topic.yaml
kubectl get kafkatopic -n kafka
```

### E. Verify produce/consume (cloud terminal)

Split into two terminals, or run sequentially.

Producer:
```powershell
kubectl -n kafka run producer -ti --rm --restart=Never `
  --image=quay.io/strimzi/kafka:1.3.1-kafka-4.3.2 -- `
  bin/kafka-console-producer.sh `
  --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic test-topic
```
Type: `hello from cloud terminal`, `message-2`, `message-3`, then Ctrl+C.

Consumer:
```powershell
kubectl -n kafka run consumer -ti --rm --restart=Never `
  --image=quay.io/strimzi/kafka:1.3.1-kafka-4.3.2 -- `
  bin/kafka-console-consumer.sh `
  --bootstrap-server my-cluster-kafka-bootstrap:9092 `
  --topic test-topic --from-beginning --group demo-group
```

### F. Deploy Kafka UI and open it

Quick approach (port-forward):
```powershell
kubectl -n kafka create deployment kafka-ui --image=kafbat/kafka-ui:latest
kubectl -n kafka set env deployment/kafka-ui `
  KAFKA_CLUSTERS_0_NAME=aks-kafka `
  KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=my-cluster-kafka-bootstrap:9092
kubectl -n kafka expose deployment kafka-ui --port=8080

kubectl -n kafka port-forward svc/kafka-ui 8080:8080
```
Open http://localhost:8080 → cluster online → **Topics → test-topic → messages** shows your cloud-terminal messages → **Consumers → demo-group** shows offsets.

After this manual run works, keep the resources and move to the automated flow (charts → Argo CD → Jenkins). Argo CD will adopt/replace them.

### G. Cleanup (avoid Azure cost)

```powershell
kubectl delete -f kafka-local/k8s/topic.yaml
kubectl delete -f kafka-local/k8s/kafka-cluster.yaml
helm uninstall strimzi-kafka-operator -n kafka
kubectl delete namespace kafka
az group delete --name $RG --yes --no-wait
```

---

## 3. Target architecture (delivery flow)

```
Developer
  │ git push
  ▼
GitHub repository (source of truth: Helm charts + manifests)
  │ webhook
  ▼
Jenkins (CI)
  │  - lint/validate Helm charts & manifests
  │  - render/template check (helm template)
  │  - optionally build images / run tests
  │  - commit environment values updates (e.g., image tags) if needed
  ▼
Argo CD (CD / GitOps)
  │  - watches GitHub path(s) (Helm chart + env values)
  │  - syncs desired state to cluster
  ▼
AKS cluster
  ├─ Strimzi operator (installed via Helm)
  ├─ Kafka cluster (KafkaNodePool + Kafka CRs, via Helm chart)
  ├─ KafkaTopic CR(s)
  └─ Kafka UI (Deployment + Service + Ingress)
```

**Responsibilities:**
- **GitHub** — single source of truth. Argo CD syncs from Git, not from Jenkins.
- **Jenkins** — CI: validate, lint, template, test; produce artifacts/values updates; trigger/notify. (Jenkins does **not** deploy to AKS directly in this design.)
- **Argo CD** — CD: applies desired state to AKS (GitOps, drift detection, rollbacks).
- **Helm** — packaging/templating for all Kubernetes resources (Strimzi operator optional via Helm; Kafka/UI as charts).
- **AKS** — runtime.

> Why this split: Jenkins validates and builds; Argo CD is the only thing that changes the cluster. This gives auditability, easy rollbacks (`git revert`), and drift detection.

---

## 4. Repository layout (proposed)

```
kafka-platform/
├── charts/
│   ├── kafka/                     # Helm chart for Kafka CRs (Strimzi)
│   │   ├── Chart.yaml
│   │   ├── values.yaml
│   │   └── templates/
│   │       ├── kafkanodepool.yaml
│   │       ├── kafka.yaml
│   │       └── kafkatopic.yaml
│   └── kafka-ui/                  # Helm chart for Kafka UI
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
│           ├── deployment.yaml
│           ├── service.yaml
│           └── ingress.yaml
├── environments/
│   ├── dev/
│   │   ├── kafka-values.yaml
│   │   ├── kafka-ui-values.yaml
│   │   └── kustomization? (optional)
│   └── prod/
│       ├── kafka-values.yaml
│       └── kafka-ui-values.yaml
├── argocd/
│   ├── app-kafka.yaml             # Argo CD Application (Kafka chart)
│   └── app-kafka-ui.yaml          # Argo CD Application (UI chart)
├── jenkins/
│   └── Jenkinsfile
└── README.md
```

- `charts/` — reusable Helm charts.
- `environments/<env>/*-values.yaml` — per-environment configuration (replicas, storage class/size, listeners, UI host, image tags).
- `argocd/` — Argo CD `Application` manifests (or use the Argo CD UI/CLI to create them).
- `jenkins/Jenkinsfile` — CI pipeline.

---

## 5. What each piece does (concrete)

### 5.1 GitHub
- Stores all charts, values, Argo CD app manifests, Jenkinsfile.
- Branching: e.g., `main` → dev/prod depending on your promotion model (env branches, or folder-per-env with Argo CD tracking `main`).
- Jenkins gets a webhook on push; Argo CD also watches the repo (poll/webhook).

### 5.2 Jenkins (CI)
Typical pipeline stages:
1. **Checkout** — pull the commit.
2. **Tooling** — `helm version`, `kubectl version --client` (kubectl not strictly needed for CI if only templating).
3. **Lint/validate**
   - `helm lint charts/kafka`
   - `helm lint charts/kafka-ui`
   - `helm template charts/kafka -f environments/dev/kafka-values.yaml > /tmp/kafka-rendered.yaml`
   - `helm template charts/kafka-ui -f environments/dev/kafka-ui-values.yaml > /tmp/ui-rendered.yaml`
   - optional: `kubeconform`/`kubeval` on rendered YAML (schema validation; CRDs need skip/extra schemas).
4. **(Optional) Build/test** — if you build custom images (e.g., Kafka Connect with plugins), build/push to a registry here.
5. **(Optional) Update env values** — e.g., bump image tags in `environments/dev/*-values.yaml` and commit back (triggers Argo CD sync).
6. **Notify** — Slack/email; report success/failure.

**Jenkins credentials:** kubeconfig is **not** needed for deployment in this design (Argo CD does it). Jenkins only needs repo access (and registry creds if building images).

### 5.3 Argo CD (CD)
- Installed on AKS (Helm chart or manifests), typically namespace `argocd`.
- Each `Application` points to:
  - **source**: GitHub repo URL, path to chart (`charts/kafka`, `charts/kafka-ui`), and values file(s) in `environments/dev/...`
  - **destination**: AKS cluster + namespace `kafka`
  - **syncPolicy**: automated (prune/self-heal) for dev
- Argo CD continuously reconciles Git → cluster. A `git push` (or Jenkins commit) triggers sync via webhook/poll.

### 5.4 Helm (packaging)
- `charts/kafka` templates the Strimzi CRs (`KafkaNodePool`, `Kafka`, `KafkaTopic`) from values:
  ```yaml
  kafka:
    version: 4.3.2
    nodePool:
      replicas: 3
      roles: [controller, broker]
    storage:
      class: managed-csi
      size: 20Gi
    topic:
      name: test-topic
      partitions: 3
      replicas: 3
  ```
- `charts/kafka-ui` templates Kafka UI Deployment/Service/Ingress from values (image, host, bootstrap servers).
- Strimzi operator itself: install once via Helm (`strimzi/strimzi-kafka-operator`) — can also be an Argo CD app or a separate Helm release managed outside this pipeline (keep it simple: operator via Helm, apps via Argo CD).

---

## 6. Concrete artifacts (starting templates)

### 6.1 `argocd/app-kafka.yaml`

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: kafka-dev
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/<ORG>/<REPO>.git
    targetRevision: main
    path: charts/kafka
    helm:
      valueFiles:
        - ../../environments/dev/kafka-values.yaml
  destination:
    server: https://kubernetes.default.svc
    namespace: kafka
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

### 6.2 `argocd/app-kafka-ui.yaml`

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: kafka-ui-dev
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/<ORG>/<REPO>.git
    targetRevision: main
    path: charts/kafka-ui
    helm:
      valueFiles:
        - ../../environments/dev/kafka-ui-values.yaml
  destination:
    server: https://kubernetes.default.svc
    namespace: kafka
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

> Note: Argo CD restricts value file paths outside the chart path depending on version/settings. Safer pattern: keep values **inside** the chart (`charts/kafka/values-dev.yaml`) or use an umbrella chart per environment. If Argo CD complains, switch to per-env values files inside the chart directory.

### 6.3 `jenkins/Jenkinsfile` (minimal CI)

```groovy
pipeline {
  agent any
  environment {
    KAFKA_CHART = 'charts/kafka'
    UI_CHART    = 'charts/kafka-ui'
    ENV_DIR     = 'environments/dev'
  }
  stages {
    stage('Checkout') {
      steps { checkout scm }
    }
    stage('Helm lint') {
      steps {
        sh 'helm lint $KAFKA_CHART'
        sh 'helm lint $UI_CHART'
      }
    }
    stage('Render (dry-run)') {
      steps {
        sh 'helm template kafka $KAFKA_CHART -f $ENV_DIR/kafka-values.yaml > /tmp/kafka.yaml'
        sh 'helm template kafka-ui $UI_CHART -f $ENV_DIR/kafka-ui-values.yaml > /tmp/kafka-ui.yaml'
        sh 'cat /tmp/kafka.yaml | head -n 40'
        sh 'cat /tmp/kafka-ui.yaml | head -n 40'
      }
    }
    stage('Validate YAML') {
      steps {
        sh 'kubeconform -summary -ignore-missing-schemas /tmp/kafka.yaml /tmp/kafka-ui.yaml || true'
      }
    }
  }
  post {
    success { echo 'CI passed — Argo CD will sync from Git.' }
    failure { echo 'CI failed.' }
  }
}
```

### 6.4 `environments/dev/kafka-values.yaml` (example)

```yaml
kafka:
  version: 4.3.2
  nodePool:
    replicas: 1          # change to 3 for HA
    roles: [controller, broker]
  storage:
    class: managed-csi
    size: 20Gi
    deleteClaim: false
  replication:
    default: 1           # 3 with 3 nodes
    offsets: 1
    transaction: 1
    minIsr: 1            # 2 when using replication factor 3
  listeners:
    - name: plain
      port: 9092
      type: internal
      tls: false
  topic:
    name: test-topic
    partitions: 1
    replicas: 1
```

### 6.5 `environments/dev/kafka-ui-values.yaml` (example)

```yaml
image:
  repository: kafbat/kafka-ui
  tag: latest
bootstrapServers: my-cluster-kafka-bootstrap:9092
clusterName: aks-kafka
ingress:
  enabled: false        # enable + host when you have an ingress controller/DNS
  className: nginx
  host: kafka-ui.example.com
```

---

## 7. End-to-end flow (what happens on a change)

1. You edit a chart/values file and `git push` to GitHub.
2. GitHub webhook triggers **Jenkins**:
   - lints charts, renders templates, validates YAML.
   - (optional) builds/pushes images; (optional) commits value updates.
3. **Argo CD** sees the new commit (webhook/poll) and syncs the affected Application:
   - renders the Helm chart with the env values,
   - applies `KafkaNodePool`/`Kafka`/`KafkaTopic` and Kafka UI resources to AKS.
4. Strimzi reconciles the CRs into real Kafka pods/services.
5. You verify in **Kafka UI** and via producer/consumer from a cloud terminal.

---

## 8. Verification (definition of done, step by step)

### 8.1 Preconditions
- AKS cluster running; `kubectl` context = AKS.
- Strimzi operator installed (Helm) in `kafka`.
- Argo CD installed; Applications `Synced`/`Healthy`.

Check:
```powershell
kubectl config current-context
kubectl -n kafka get pods
kubectl -n argocd get applications
```

### 8.2 Kafka + UI deployed
```powershell
kubectl -n kafka get kafka,kafkanodepool,kafkatopic,svc,pods
kubectl -n kafka get pods -l app.kubernetes.io/name=kafka-ui   # adapt to chart labels
```

Expected: `my-cluster` Ready; `test-topic` exists; Kafka UI pod Running.

### 8.3 Produce messages from a cloud terminal
```powershell
kubectl -n kafka run producer -ti --rm --restart=Never \
  --image=quay.io/strimzi/kafka:1.3.1-kafka-4.3.2 -- \
  bin/kafka-console-producer.sh \
  --bootstrap-server my-cluster-kafka-bootstrap:9092 \
  --topic test-topic
```
Type:
```
hello from cloud terminal
message-2
message-3
```
(Ctrl+C to exit.)

### 8.4 Consume (optional, proves consumer group)
```powershell
kubectl -n kafka run consumer -ti --rm --restart=Never \
  --image=quay.io/strimzi/kafka:1.3.1-kafka-4.3.2 -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server my-cluster-kafka-bootstrap:9092 \
  --topic test-topic --from-beginning --group demo-group
```

### 8.5 See it in Kafka UI
Access options:
- **Quick:** `kubectl -n kafka port-forward svc/<kafka-ui-svc> 8080:8080` → http://localhost:8080
- **Proper (AKS):** Ingress + DNS (enable in `kafka-ui` chart values once ingress controller exists).

In the UI confirm:
- Cluster `aks-kafka` online (broker listed).
- **Topics → test-topic → messages** shows `hello from cloud terminal`, etc.
- **Consumers → demo-group** shows offsets/lag.

---

## 9. One-time setup checklist (in order)

### A. Azure + AKS
- [ ] `az login`; select subscription.
- [ ] Create resource group + AKS (`az aks create ...`), `az aks get-credentials`.
- [ ] Verify nodes + storage classes.

### B. Cluster prerequisites
- [ ] Install **ingress-nginx** (for UI Ingress later) — optional for first smoke test.
- [ ] Install **Argo CD** (Helm or manifests) in `argocd`.
- [ ] Install **Strimzi operator** via Helm in `kafka`.

### C. GitHub repo
- [ ] Create repo/layout (`charts/`, `environments/`, `argocd/`, `jenkins/`).
- [ ] Push charts + values + Argo CD app manifests + Jenkinsfile.

### D. Jenkins
- [ ] Create pipeline job pointing to repo (Jenkinsfile).
- [ ] Add GitHub webhook → Jenkins.
- [ ] Run pipeline; confirm lint/render succeed.

### E. Argo CD
- [ ] Add repo credentials (if private).
- [ ] Apply `argocd/app-kafka.yaml` and `argocd/app-kafka-ui.yaml` (or create via UI).
- [ ] Confirm `Synced` + `Healthy`.

### F. Verify on AKS
- [ ] Kafka Ready, topic exists.
- [ ] Produce from cloud terminal; consume.
- [ ] Kafka UI shows messages + consumer group.

---

## 10. Notes, gotchas, and decisions

- **Do not let Jenkins deploy to AKS directly** in this design — keep Argo CD as the only deployer (GitOps). Jenkins validates/builds.
- **CRD validation:** `kubeconform` doesn’t know Strimzi CRDs by default; use `-ignore-missing-schemas` or supply schemas.
- **Argo CD value file paths:** if `valueFiles` outside chart path is blocked, move env values inside the chart (e.g., `charts/kafka/values-dev.yaml`) or use an umbrella chart per env.
- **Kafka versions:** keep `spec.kafka.version` aligned with the Strimzi operator version installed on AKS (check `STRIMZI_KAFKA_IMAGES`).
- **Storage:** use `managed-csi` (or premium) and non-trivial sizes; `deleteClaim: false` in real envs to avoid accidental data loss.
- **Replicas:** smoke test with 1 replica; then move to 3 brokers/controllers, replication factor 3, `min.insync.replicas: 2`.
- **External access:** start internal + port-forward; add LoadBalancer/Ingress + TLS/auth later.
- **Secrets:** don't commit credentials. Use Jenkins credentials, Argo CD repo creds, and Kubernetes Secrets (or external secret manager).
- **Kafka UI Ingress:** requires an ingress controller and DNS; for the first milestone, port-forward is acceptable proof.
- **Images:** `kafbat/kafka-ui:latest` and Strimzi Kafka images are public. In stricter environments, mirror to your registry and pin versions (avoid `latest`).

---

## 11. Minimal milestone plan (recommended order)

1. **Milestone 1 — AKS baseline:** AKS up, Strimzi via Helm, Kafka CRs applied manually (reuse local manifests). Prove produce/consume + UI via port-forward on AKS.
2. **Milestone 2 — Charts:** convert manifests into `charts/kafka` and `charts/kafka-ui` with values; deploy via `helm install` manually to validate.
3. **Milestone 3 — GitOps:** install Argo CD; create Applications pointing to GitHub; prove a `git push` changes AKS.
4. **Milestone 4 — CI:** add Jenkins pipeline (lint/render/validate) with GitHub webhook; keep deploy via Argo CD.
5. **Milestone 5 — UI exposure:** add Ingress (ingress-nginx) + DNS for Kafka UI; verify in browser.
6. **Milestone 6 — HA/hardening:** 3 nodes, replication 3, TLS/auth, monitoring, Connect (if needed).

Milestone 1 proves the outcome you asked for; 2–5 automate it.

---

## 12. Quick command reference (verification)

```powershell
# contexts / cluster
kubectl config get-contexts
kubectl config use-context <aks-context>
kubectl get nodes

# strimzi + kafka
kubectl -n kafka get pods
kubectl -n kafka get kafka,kafkanodepool,kafkatopic
kubectl -n kafka get svc

# argocd
kubectl -n argocd get applications
kubectl -n argocd get pods

# produce / consume
kubectl -n kafka run producer -ti --rm --restart=Never --image=quay.io/strimzi/kafka:1.3.1-kafka-4.3.2 -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic test-topic
kubectl -n kafka run consumer -ti --rm --restart=Never --image=quay.io/strimzi/kafka:1.3.1-kafka-4.3.2 -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic test-topic --from-beginning --group demo-group

# kafka ui
kubectl -n kafka port-forward svc/<kafka-ui-service> 8080:8080
```
