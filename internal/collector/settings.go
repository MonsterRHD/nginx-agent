// Copyright (c) F5, Inc.
//
// This source code is licensed under the Apache License, Version 2.0 license found in the
// LICENSE file in the root directory of this source tree.
package collector

import (
	"log/slog"

	"github.com/nginx/agent/v3/internal/config"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/httpprovider"
	"go.opentelemetry.io/collector/confmap/provider/httpsprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/otelcol"
)

func OTelCollectorSettings(cfg *config.Config) otelcol.CollectorSettings {
	return otelcol.CollectorSettings{
		Factories:               OTelComponentFactories,
		BuildInfo:               BuildInfo(cfg),
		DisableGracefulShutdown: false,
		ConfigProviderSettings:  ConfigProviderSettings(cfg),
		LoggingOptions:          nil,
		SkipSettingGRPCLogger:   false,
	}
}

// ConfigProviderSettings are the settings to configure the behavior of the ConfigProvider.
func ConfigProviderSettings(cfg *config.Config) otelcol.ConfigProviderSettings {
	return otelcol.ConfigProviderSettings{
		ResolverSettings: confmap.ResolverSettings{
			ProviderFactories:  createProviderFactories(),
			ConverterFactories: createConverterFactories(),
			URIs:               createURIs(cfg),
		},
	}
}

func createProviderFactories() []confmap.ProviderFactory {
	providerConfig := []confmap.ProviderFactory{
		envprovider.NewFactory(),
		fileprovider.NewFactory(),
		httpprovider.NewFactory(),
		httpsprovider.NewFactory(),
		yamlprovider.NewFactory(),
	}

	return providerConfig
}

func createConverterFactories() []confmap.ConverterFactory {
	converterConfig := []confmap.ConverterFactory{}

	return converterConfig
}

func createURIs(cfg *config.Config) []string {
	configFiles := configURIs(cfg.Collector.ConfigPath, cfg.Collector.AdditionalConfigPaths)
	slog.Info("Merging additional OTel config files", "config_files", configFiles)

	return configFiles
}
