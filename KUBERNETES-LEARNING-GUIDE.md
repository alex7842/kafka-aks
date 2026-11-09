# Local Kubernetes + Kafka (Strimzi) — Learning Guide

This guide explains what you should understand **before** running Kafka on Kubernetes locally (and later moving to AKS with Helm + Strimzi). It builds on what you already did with Docker Compose (`broker`, `kafka-ui`, produce/consume).

---

## 1) What Kubernetes is (in plain terms)

Kubernetes (K8s) is a **container orchestration platform**. Instead of running containers manually with `docker run`/`docker compose`, you tell Kubernetes the *desired state* (e.g., “run 1 Kafka broker, expose it internally, keep it healthy”), and Kubernetes works to maintain that state.

**Docker Compose vs Kubernetes (mental model):**

| Docker Compose (local) | Kubernetes (local/AKS) |
|---|---|
| `docker-compose.yml` defines services | YAML manifests define resources (Pod, Deployment, Service, etc.) |
| `docker compose up -d` starts containers | `kubectl apply -f ...` creates/updates resources |
| Container restarts via `restart: unless-stopped` | Controllers restart/recreate Pods to match desired state |
| Network is usually a Compose network | Cluster networking + `Service` objects provide stable access |
| Volumes in compose | `PersistentVolumeClaim` (PVC) requests storage |
| You manage app-specific logic | Operators (like Strimzi) manage app-specific logic |

---

## 2) How local Kubernetes works

“Local Kubernetes” means running a Kubernetes cluster **on your machine** for development/testing.

### Common options
- **Docker Desktop Kubernetes** (Windows/Mac): enable Kubernetes in Docker Desktop settings. Easy if you already use Docker Desktop.
- **kind** (Kubernetes in Docker): runs K8s nodes as Docker containers. Lightweight, popular for CI/local dev.
- **minikube**: runs a local cluster (can use Docker/other drivers).

### What’s happening under the hood
- You get a **control plane** (API server, scheduler, controller manager, etc.) and at least one **worker node**.
- `kubectl` talks to the cluster’s **API server** (configured via your kubeconfig, usually `~/.kube/config`).
- When you `kubectl apply` a manifest, the API server stores the desired state; controllers schedule Pods onto nodes and manage them.
- Pods run containers (using a container runtime). On Docker Desktop/kind, Docker is involved behind the scenes.

**Important:** Local K8s is for learning/testing. AKS is managed Kubernetes in Azure (Azure manages much of the control plane; you manage node pools/workloads and cloud integrations).

---

## 3) Core Kubernetes concepts you need before Strimzi

You don’t need to master all of Kubernetes, but you should be comfortable with these:

### 3.1 `kubectl` basics
- `kubectl get pods|svc|deployments|ns -A`
- `kubectl describe pod <name> -n <ns>` (debugging)
- `kubectl logs <pod> -n <ns>` / `kubectl logs -f ...`
- `kubectl apply -f file.yaml` / `kubectl delete -f file.yaml`
- `kubectl exec -it <pod> -n <ns> -- sh` (or `bash`)
- `kubectl port-forward ...` (access internal services from your laptop)

### 3.2 Namespaces
- Logical grouping of resources (e.g., `kafka`, `default`).
- Helps organize and avoid name collisions.
- Most commands need `-n <namespace>` (or set a default context namespace).

### 3.3 Pods
- Smallest deployable unit in K8s; usually runs 1 container (sometimes sidecars).
- Pods are **ephemeral** — they can be recreated. Don’t rely on a Pod’s IP/name staying the same.

### 3.4 Workload controllers (know what they do)
- **Deployment**: manages stateless replicas (good for apps like Kafka UI).
- **StatefulSet**: manages stateful apps with stable identities/storage patterns (Kafka brokers are stateful; Strimzi creates/manages these under the hood).
- **Job/CronJob**: run-to-completion / scheduled tasks.

### 3.5 Services (networking)
- Provides a **stable DNS name/IP** to reach Pods (whose IPs change).
- Common types:
  - `ClusterIP` (default): internal-only access inside the cluster.
  - `NodePort` / `LoadBalancer`: expose outside the cluster (behavior differs locally vs AKS).
- In-cluster DNS looks like: `my-service.my-namespace.svc.cluster.local` (often shortened to `my-service.my-namespace.svc` or `my-service` within the same namespace).

**Why this matters for Kafka:** Clients/connectors/UI connect to a stable bootstrap address (e.g., `my-cluster-kafka-bootstrap:9092`), not individual Pod IPs.

### 3.6 Config and secrets
- **ConfigMap**: non-sensitive configuration data.
- **Secret**: sensitive data (passwords, certs/tokens). Base64-encoded by default, not encrypted unless you configure that.
- Strimzi uses Secrets/ConfigMaps internally (e.g., TLS certs, configs).

### 3.7 Storage
- **PersistentVolume (PV)** / **PersistentVolumeClaim (PVC)**: how stateful workloads request durable storage.
- Locally, your cluster provides a default `StorageClass` (Docker Desktop/kind/minikube do). On AKS, you’ll choose Azure disk/file storage classes and size/performance appropriately.

**Why this matters for Kafka:** Brokers store log segments/data on disk. If storage isn’t configured correctly, data can be lost or Pods can fail to start.

### 3.8 YAML manifests and desired state
- K8s resources are declared in YAML (`apiVersion`, `kind`, `metadata`, `spec`).
- `kubectl apply` is declarative: you describe what you want; K8s reconciles actual state to match.

---

## 4) Operators and Custom Resource Definitions (CRDs) — key for Strimzi

This is the biggest conceptual jump from Docker Compose.

- A **CRD** extends Kubernetes with new resource types (e.g., `Kafka`, `KafkaTopic`, `KafkaConnect`).
- An **Operator** is a controller that watches those custom resources and performs app-specific operations (create StatefulSets/Services/Configs, handle rolling updates, reconcile topics/users, etc.).

**Strimzi provides CRDs like:**
- `Kafka` / `KafkaNodePool` — define the Kafka cluster (brokers/controllers, listeners, storage, config). With recent Strimzi, KRaft uses node pools/roles.
- `KafkaTopic` — manage topics declaratively.
- `KafkaUser` — manage users/ACLs/quotas (when using auth).
- `KafkaConnect` / `KafkaConnector` — run Kafka Connect and manage connectors.

**Mapping from your Docker setup:**
- `confluentinc/cp-kafka` container → `Kafka` + `KafkaNodePool` managed by Strimzi
- `kafka-topics.sh` CLI → `KafkaTopic` CR (or CLI inside a pod)
- `kafka-connect` container + REST/scripts → `KafkaConnect` + `KafkaConnector` CRs
- `docker compose up/down` → `kubectl apply/delete` + operator reconciliation

---

## 5) Helm (you’ll use it on AKS)

**Helm** is a package manager for Kubernetes.
- A **chart** is a packaged set of K8s templates/manifests.
- You install/upgrade releases with `helm install` / `helm upgrade`.

**Why it matters:** Strimzi is commonly installed via its Helm chart, especially in team/AKS environments. You still create Strimzi custom resources (`Kafka`, etc.) as YAML after the operator is installed.

Basics to know:
- `helm repo add`, `helm repo update`, `helm search repo`
- `helm install <release> <chart> -n <ns>`
- `helm upgrade --install ...` (idempotent pattern)
- `helm values` / `-f values.yaml` (customize chart settings)

---

## 6) What to learn before implementing locally

### Must-know (before Step 1)
- [ ] `kubectl` basics: `get`, `describe`, `logs`, `apply`, `delete`, `exec`, `port-forward`
- [ ] Pods vs Services (ephemeral Pods, stable Service DNS)
- [ ] Namespaces
- [ ] How to read K8s YAML (`kind`, `metadata`, `spec`)
- [ ] How to check cluster health: `kubectl get nodes`, `kubectl get pods -A`

### Should-know (makes Strimzi much easier)
- [ ] Deployments/StatefulSets at a high level
- [ ] PVC/StorageClass basics
- [ ] ConfigMaps/Secrets basics
- [ ] CRDs and Operators (what they are and why Strimzi uses them)
- [ ] Events/troubleshooting: `kubectl describe`, `kubectl get events -n <ns> --sort-by=.lastTimestamp`

### Nice-to-have (for AKS later)
- [ ] Helm basics
- [ ] Ingress / LoadBalancer concepts
- [ ] Resource requests/limits
- [ ] RBAC basics (who can do what)
- [ ] Azure/AKS specifics: node pools, StorageClasses, networking, `az aks get-credentials`

---

## 7) Practical prerequisites on your machine

1. **Local K8s cluster running** (Docker Desktop Kubernetes, kind, or minikube)
2. **`kubectl` installed and pointing to that cluster**
   ```powershell
   kubectl cluster-info
   kubectl get nodes
   ```
3. **Enough resources**: Kafka + operator can be memory-hungry. For local learning, allocate sufficient CPU/RAM to Docker Desktop/WSL2 (e.g., 4+ GB RAM available; more is better).
4. **Internet access** to pull images/manifests (Strimzi operator, Kafka images, UI images).

---

## 8) Suggested learning path (short and practical)

1. **K8s fundamentals (1–2 hours):** run a simple `Deployment` + `Service` (e.g., nginx), use `port-forward`, scale it, view logs. Goal: be comfortable with `kubectl` and the Pod/Service model.
2. **Install Strimzi operator locally:** learn what an operator/CRD is by seeing `kubectl get crds | findstr kafka`.
3. **Deploy a single-node KRaft Kafka (`Kafka` + `KafkaNodePool`)**: watch Pods come up, inspect Services (`my-cluster-kafka-bootstrap`).
4. **Create a `KafkaTopic` and test produce/consume** from a temporary pod — same flow you validated in Docker.
5. **Add Kafka UI** via port-forward.
6. **Move to AKS:** install Strimzi with Helm, reuse manifests with AKS-appropriate storage/replicas/listeners.

---

## 9) Common pitfalls to expect

- **Version compatibility:** Strimzi supports specific Kafka versions. Always check Strimzi docs/release notes for the `Kafka`/`KafkaNodePool` schema and supported versions.
- **KRaft config changes across Strimzi versions:** Strimzi’s KRaft setup has evolved (node pools, `strimzi.io/kraft` annotation/handling). Follow docs for *your* Strimzi version.
- **Storage issues:** Pods stuck in `Pending` often means PVC/StorageClass problems.
- **Networking confusion:** `localhost:9092` works in Docker Compose on your host; in K8s, clients inside the cluster use the Service DNS. From your laptop, you usually use `kubectl port-forward` or an external listener.
- **Resource constraints locally:** broker/operator may crashloop if Docker Desktop doesn’t have enough memory.

---

## 10) Quick checklist before you implement

- [ ] Local Kubernetes is enabled and `kubectl get nodes` works
- [ ] You can create/apply a simple manifest and view logs
- [ ] You understand: Pod, Service, Namespace, PVC (basics)
- [ ] You understand: CRD + Operator (Strimzi will install CRDs and reconcile them)
- [ ] You’ve decided local cluster type: Docker Desktop Kubernetes / kind / minikube

Once these are checked, you’re ready to install Strimzi and deploy a single-node Kafka cluster locally.
