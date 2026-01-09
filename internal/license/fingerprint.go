package license

import (
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"strings"
)

// GenerateMachineFingerprint generates a unique fingerprint for this machine
// Format: sha256:<64 hex characters>
func GenerateMachineFingerprint() (string, error) {
	components := []string{}

	// Get hostname
	hostname, err := os.Hostname()
	if err == nil && hostname != "" {
		components = append(components, fmt.Sprintf("hostname:%s", hostname))
	}

	// Get MAC addresses (sorted for consistency)
	macs := getMACAddresses()
	if len(macs) > 0 {
		components = append(components, fmt.Sprintf("macs:%s", strings.Join(macs, ",")))
	}

	// Get CPU info
	cpuInfo := getCPUInfo()
	if cpuInfo != "" {
		components = append(components, fmt.Sprintf("cpu:%s", cpuInfo))
	}

	// Get OS info
	osInfo := fmt.Sprintf("os:%s/%s", runtime.GOOS, runtime.GOARCH)
	components = append(components, osInfo)

	// If we couldn't get any useful info, return an error
	if len(components) < 2 {
		return "", fmt.Errorf("unable to gather enough machine information for fingerprint")
	}

	// Sort components for consistency
	sort.Strings(components)

	// Create fingerprint string
	fingerprintData := strings.Join(components, "|")

	// Hash it
	hash := sha256.Sum256([]byte(fingerprintData))

	return fmt.Sprintf("sha256:%x", hash), nil
}

// getMACAddresses returns sorted list of MAC addresses from stable physical interfaces only.
// Virtual interfaces (VMs, Docker, bridges, etc.) are excluded to ensure fingerprint stability.
func getMACAddresses() []string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil
	}

	var macs []string
	for _, iface := range interfaces {
		// Skip loopback interfaces
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		// Skip virtual/unstable interfaces that can change between restarts
		if isVirtualInterface(iface.Name) {
			continue
		}

		mac := iface.HardwareAddr.String()
		if mac != "" && mac != "00:00:00:00:00:00" {
			macs = append(macs, mac)
		}
	}

	sort.Strings(macs)
	return macs
}

// isVirtualInterface returns true if the interface name indicates a virtual/unstable interface
// that should be excluded from fingerprint generation.
func isVirtualInterface(name string) bool {
	// Virtual/bridge interfaces that can change between restarts
	virtualPrefixes := []string{
		"docker",   // Docker networks
		"br-",      // Docker/Linux bridges
		"veth",     // Virtual ethernet (Docker containers)
		"vmnet",    // VMware networks
		"vmenet",   // macOS VM networks
		"bridge",   // Bridge interfaces (Linux, macOS)
		"virbr",    // libvirt bridges
		"vboxnet",  // VirtualBox networks
		"utun",     // VPN tunnels
		"tun",      // TUN devices
		"tap",      // TAP devices
		"awdl",     // Apple Wireless Direct Link (unstable)
		"llw",      // Apple Low Latency WLAN (unstable)
		"ap",       // Apple access point (hotspot)
		"anpi",     // Apple network processor interface
	}

	for _, prefix := range virtualPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}

	return false
}

// getCPUInfo returns CPU information string
func getCPUInfo() string {
	// Use number of CPUs as a simple identifier
	numCPU := runtime.NumCPU()
	return fmt.Sprintf("cores:%d", numCPU)
}

// ValidateFingerprint checks if a fingerprint is in the correct format
func ValidateFingerprint(fingerprint string) bool {
	if !strings.HasPrefix(fingerprint, "sha256:") {
		return false
	}

	hexPart := strings.TrimPrefix(fingerprint, "sha256:")
	if len(hexPart) != 64 {
		return false
	}

	// Check if it's valid hex
	for _, c := range hexPart {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}

	return true
}
