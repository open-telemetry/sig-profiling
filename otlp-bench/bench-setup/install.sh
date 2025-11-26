#!/bin/bash
set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

CRI_DOCKERD_VERSION="0.3.21"
CNI_PLUGIN_VERSION="v1.8.0"
KUBERNETES_VERSION="v1.34"

# =============================================================================
# Helper Functions
# =============================================================================

log_section() {
    echo ""
    echo "============================================"
    echo "$1"
    echo "============================================"
}

log_info() {
    echo "→ $1"
}

log_success() {
    echo "✓ $1"
}

get_arch() {
    local arch
    arch=$(dpkg --print-architecture)
    case "$arch" in
        amd64|arm64)
            echo "$arch"
            ;;
        *)
            echo "Unsupported architecture: $arch" >&2
            exit 1
            ;;
    esac
}

# =============================================================================
# Main Script
# =============================================================================

echo "Installing requirements for minikube with driver=none on Ubuntu host..."

ARCH=$(get_arch)
log_info "Detected architecture: $ARCH"

# =============================================================================
# Section 1: Base System Packages
# =============================================================================

log_section "Installing base system packages"

sudo apt-get update
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    wget \
    conntrack \
    socat \
    ebtables \
    ethtool

# =============================================================================
# Section 2: Docker
# =============================================================================

log_section "Installing Docker"

if ! command -v docker &> /dev/null; then
    log_info "Adding Docker apt repository..."
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg

    echo \
      "deb [arch=$ARCH signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    sudo apt-get update

    log_info "Installing Docker packages..."
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    log_success "Docker installed"
else
    log_success "Docker already installed"
fi

# Ensure current user can access Docker socket without sudo
log_info "Adding $USER to docker group..."
sudo usermod -aG docker "$USER"
log_success "Added $USER to docker group (requires re-login to take effect)"

# =============================================================================
# Section 3: Kubernetes Tools (from official apt repository)
# =============================================================================

log_section "Installing Kubernetes tools from apt repository"

if [ ! -f /etc/apt/sources.list.d/kubernetes.list ]; then
    log_info "Adding Kubernetes apt repository ($KUBERNETES_VERSION)..."
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL "https://pkgs.k8s.io/core:/stable:/$KUBERNETES_VERSION/deb/Release.key" | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
    echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/$KUBERNETES_VERSION/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list
    sudo apt-get update
else
    log_info "Kubernetes apt repository already configured"
fi

log_info "Installing cri-tools and kubectl..."
sudo apt-get install -y cri-tools kubectl

# =============================================================================
# Section 4: cri-dockerd (Docker CRI shim for Kubernetes v1.24+)
# =============================================================================

log_section "Installing cri-dockerd"

if ! command -v cri-dockerd &> /dev/null; then
    UBUNTU_CODENAME=$(lsb_release -cs)
    log_info "Detected Ubuntu $UBUNTU_CODENAME ($ARCH)"

    # Map unsupported codenames to compatible ones
    case "$UBUNTU_CODENAME" in
        noble)
            log_info "Using jammy package for noble (24.04)"
            UBUNTU_CODENAME="jammy"
            ;;
    esac

    DEB_FILE="cri-dockerd_${CRI_DOCKERD_VERSION}.3-0.ubuntu-${UBUNTU_CODENAME}_${ARCH}.deb"
    DEB_URL="https://github.com/Mirantis/cri-dockerd/releases/download/v${CRI_DOCKERD_VERSION}/${DEB_FILE}"

    log_info "Downloading cri-dockerd from $DEB_URL"
    wget -q "$DEB_URL"

    sudo dpkg -i "$DEB_FILE" || sudo apt-get install -f -y
    rm "$DEB_FILE"

    sudo systemctl enable cri-docker.socket
    sudo systemctl start cri-docker.service
    log_success "cri-dockerd installed and started"
else
    log_success "cri-dockerd already installed"
fi

# =============================================================================
# Section 5: CNI Plugins (required for Kubernetes networking)
# =============================================================================

log_section "Installing CNI plugins"

CNI_PLUGIN_INSTALL_DIR="/opt/cni/bin"

if [ ! -d "$CNI_PLUGIN_INSTALL_DIR" ] || [ -z "$(ls -A $CNI_PLUGIN_INSTALL_DIR 2>/dev/null)" ]; then
    CNI_PLUGIN_TAR="cni-plugins-linux-${ARCH}-${CNI_PLUGIN_VERSION}.tgz"
    CNI_PLUGIN_URL="https://github.com/containernetworking/plugins/releases/download/${CNI_PLUGIN_VERSION}/${CNI_PLUGIN_TAR}"

    log_info "Downloading CNI plugins ${CNI_PLUGIN_VERSION} for ${ARCH}..."
    curl -LO "$CNI_PLUGIN_URL"

    sudo mkdir -p "$CNI_PLUGIN_INSTALL_DIR"
    sudo tar -xf "$CNI_PLUGIN_TAR" -C "$CNI_PLUGIN_INSTALL_DIR"
    rm "$CNI_PLUGIN_TAR"

    log_success "CNI plugins installed to $CNI_PLUGIN_INSTALL_DIR"
else
    log_success "CNI plugins already installed"
fi

# =============================================================================
# Section 6: Minikube (downloaded directly, not in apt repository)
# =============================================================================

log_section "Installing minikube"

if ! command -v minikube &> /dev/null; then
    MINIKUBE_URL="https://storage.googleapis.com/minikube/releases/latest/minikube-linux-${ARCH}"

    log_info "Downloading minikube for ${ARCH}..."
    curl -LO "$MINIKUBE_URL"

    sudo install "minikube-linux-${ARCH}" /usr/local/bin/minikube
    rm "minikube-linux-${ARCH}"

    log_success "minikube installed"
else
    log_success "minikube already installed"
fi

# =============================================================================
# Section 7: Helm (from official apt repository)
# =============================================================================

log_section "Installing Helm"

if ! command -v helm &> /dev/null; then
    if [ ! -f /etc/apt/sources.list.d/helm-stable-debian.list ]; then
        log_info "Adding Helm apt repository..."
        curl -fsSL https://packages.buildkite.com/helm-linux/helm-debian/gpgkey | gpg --dearmor | sudo tee /usr/share/keyrings/helm.gpg > /dev/null
        echo "deb [signed-by=/usr/share/keyrings/helm.gpg] https://packages.buildkite.com/helm-linux/helm-debian/any/ any main" | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list > /dev/null
        sudo apt-get update
    fi

    log_info "Installing Helm..."
    sudo apt-get install -y helm
    log_success "Helm installed"
else
    log_success "Helm already installed"
fi

# =============================================================================
# Section 8: System Configuration for Kubernetes
# =============================================================================

log_section "Configuring system for Kubernetes"

# Disable swap (required for Kubernetes)
log_info "Disabling swap..."
sudo swapoff -a
sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab

# Load required kernel modules
log_info "Loading kernel modules (br_netfilter, overlay)..."
sudo modprobe br_netfilter
sudo modprobe overlay

cat <<EOF | sudo tee /etc/modules-load.d/k8s.conf > /dev/null
br_netfilter
overlay
EOF

# Configure sysctl for Kubernetes networking
log_info "Configuring sysctl parameters..."
cat <<EOF | sudo tee /etc/sysctl.d/k8s.conf > /dev/null
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF

sudo sysctl --system > /dev/null

# Ensure Docker is running
log_info "Ensuring Docker is running..."
sudo systemctl start docker
sudo systemctl enable docker

# =============================================================================
# Summary
# =============================================================================

log_section "Installation complete!"

echo ""
echo "Versions installed:"
echo "  Docker:      $(docker --version 2>/dev/null | cut -d' ' -f3 | tr -d ',')"
echo "  kubectl:     $(kubectl version --client -o yaml 2>/dev/null | grep gitVersion | cut -d: -f2 | tr -d ' ')"
echo "  minikube:    $(minikube version --short 2>/dev/null)"
echo "  helm:        $(helm version --short 2>/dev/null)"
echo "  cri-dockerd: $(cri-dockerd --version 2>&1 | head -1)"
echo ""
echo "⚠️  To activate docker group membership, either:"
echo "    - Log out and back in, OR"
echo "    - Run: newgrp docker"
echo ""
echo "Then start minikube with:"
echo "  minikube start --driver=none"
