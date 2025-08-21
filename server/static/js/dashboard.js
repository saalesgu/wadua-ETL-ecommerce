class Dashboard {
    constructor() {
        this.init();
    }

    init() {
        this.loadVentas();
        $('#dashboardTabs a').on('shown.bs.tab', (e) => {
            const target = $(e.target).attr('href');
            if (target === '#productos') this.loadProductos();
            if (target === '#pagos') this.loadPagos();
        });
    }

    async loadVentas() {
        try {
            $('#ventas-content').html(`
                <div class="text-center py-5">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">Cargando...</span>
                    </div>
                    <p class="mt-2">Cargando datos de ventas...</p>
                </div>
            `);

            const response = await fetch('/api/ventas');
            const data = await response.json();
            
            if (data.success) {
                this.renderVentas(data);
            } else {
                $('#ventas-content').html('<div class="alert alert-danger">Error al cargar ventas</div>');
            }
        } catch (error) {
            $('#ventas-content').html('<div class="alert alert-danger">Error de conexión: ' + error.message + '</div>');
        }
    }

    renderVentas(data) {
        const html = `
            <div class="row mb-4">
                <div class="col-md-3">
                    <div class="card stat-card bg-primary text-white">
                        <div class="card-body text-center">
                            <h5><i class="fas fa-dollar-sign"></i> Ventas Totales</h5>
                            <h3>$${this.formatNumber(data.stats.total_ventas)}</h3>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card stat-card bg-success text-white">
                        <div class="card-body text-center">
                            <h5><i class="fas fa-shopping-bag"></i> Órdenes</h5>
                            <h3>${this.formatNumber(data.stats.total_ordenes)}</h3>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card stat-card bg-info text-white">
                        <div class="card-body text-center">
                            <h5><i class="fas fa-calendar"></i> Períodos</h5>
                            <h3>${data.stats.periodos}</h3>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card stat-card bg-warning text-white">
                        <div class="card-body text-center">
                            <h5><i class="fas fa-chart-line"></i> Promedio</h5>
                            <h3>$${this.formatNumber(data.stats.ventas_promedio)}</h3>
                        </div>
                    </div>
                </div>
            </div>

            <div class="row">
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-body">
                            <div id="chart-ventas"></div>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-body">
                            <div id="chart-ordenes"></div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="row mt-4">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">
                            <h5 class="mb-0">Datos Detallados</h5>
                        </div>
                        <div class="card-body">
                            <div class="table-responsive">
                                <table class="table table-striped">
                                    <thead>
                                        <tr>
                                            <th>Período</th>
                                            <th>Órdenes</th>
                                            <th>Ventas Totales</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${data.data.map(item => `
                                            <tr>
                                                <td>${item.periodo}</td>
                                                <td>${item.total_ordenes}</td>
                                                <td>$${this.formatNumber(item.total_ventas)}</td>
                                            </tr>
                                        `).join('')}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;

        $('#ventas-content').html(html);
        
        // Renderizar gráficos
        Plotly.newPlot('chart-ventas', data.charts.ventas.data, data.charts.ventas.layout);
        Plotly.newPlot('chart-ordenes', data.charts.ordenes.data, data.charts.ordenes.layout);
    }

    async loadProductos() {
        try {
            $('#productos-content').html(`
                <div class="text-center py-5">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">Cargando...</span>
                    </div>
                    <p class="mt-2">Cargando datos de productos...</p>
                </div>
            `);

            const response = await fetch('/api/productos');
            const data = await response.json();
            
            if (data.success) {
                this.renderProductos(data);
            } else {
                $('#productos-content').html('<div class="alert alert-danger">Error al cargar productos</div>');
            }
        } catch (error) {
            $('#productos-content').html('<div class="alert alert-danger">Error de conexión</div>');
        }
    }

    renderProductos(data) {
        const html = `
            <div class="row mb-4">
                <div class="col-md-4">
                    <div class="card stat-card bg-info text-white">
                        <div class="card-body text-center">
                            <h5><i class="fas fa-tags"></i> Categorías</h5>
                            <h3>${data.stats.total_categorias}</h3>
                        </div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card stat-card bg-success text-white">
                        <div class="card-body text-center">
                            <h5><i class="fas fa-box"></i> Unidades Vendidas</h5>
                            <h3>${this.formatNumber(data.stats.total_unidades)}</h3>
                        </div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card stat-card bg-primary text-white">
                        <div class="card-body text-center">
                            <h5><i class="fas fa-dollar-sign"></i> Ventas Totales</h5>
                            <h3>$${this.formatNumber(data.stats.ventas_totales)}</h3>
                        </div>
                    </div>
                </div>
            </div>

            <div class="row">
                <div class="col-12">
                    <div class="card">
                        <div class="card-body">
                            <div id="chart-productos"></div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="row mt-4">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">
                            <h5 class="mb-0">Top 10 Categorías</h5>
                        </div>
                        <div class="card-body">
                            <div class="table-responsive">
                                <table class="table table-striped">
                                    <thead>
                                        <tr>
                                            <th>Categoría</th>
                                            <th>Unidades Vendidas</th>
                                            <th>Ventas Totales</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${data.data.map(item => `
                                            <tr>
                                                <td>${item.Product_Category_Name}</td>
                                                <td>${this.formatNumber(item.cantidad_vendida)}</td>
                                                <td>$${this.formatNumber(item.ventas_totales)}</td>
                                            </tr>
                                        `).join('')}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;

        $('#productos-content').html(html);
        Plotly.newPlot('chart-productos', data.chart.data, data.chart.layout);
    }

    async loadPagos() {
        try {
            $('#pagos-content').html(`
                <div class="text-center py-5">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">Cargando...</span>
                    </div>
                    <p class="mt-2">Cargando datos de pagos...</p>
                </div>
            `);

            const response = await fetch('/api/pagos');
            const data = await response.json();
            
            if (data.success) {
                this.renderPagos(data);
            } else {
                $('#pagos-content').html('<div class="alert alert-danger">Error al cargar pagos</div>');
            }
        } catch (error) {
            $('#pagos-content').html('<div class="alert alert-danger">Error de conexión</div>');
        }
    }

    renderPagos(data) {
        const html = `
            <div class="row mb-4">
                <div class="col-md-4">
                    <div class="card stat-card bg-primary text-white">
                        <div class="card-body text-center">
                            <h5><i class="fas fa-credit-card"></i> Métodos</h5>
                            <h3>${data.stats.metodos_pago}</h3>
                        </div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card stat-card bg-success text-white">
                        <div class="card-body text-center">
                            <h5><i class="fas fa-exchange-alt"></i> Transacciones</h5>
                            <h3>${this.formatNumber(data.stats.total_transacciones)}</h3>
                        </div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card stat-card bg-info text-white">
                        <div class="card-body text-center">
                            <h5><i class="fas fa-money-bill-wave"></i> Total Pagado</h5>
                            <h3>$${this.formatNumber(data.stats.total_pagado)}</h3>
                        </div>
                    </div>
                </div>
            </div>

            <div class="row">
                <div class="col-12">
                    <div class="card">
                        <div class="card-body">
                            <div id="chart-pagos"></div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="row mt-4">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">
                            <h5 class="mb-0">Métodos de Pago</h5>
                        </div>
                        <div class="card-body">
                            <div class="table-responsive">
                                <table class="table table-striped">
                                    <thead>
                                        <tr>
                                            <th>Método de Pago</th>
                                            <th>Cantidad</th>
                                            <th>Total Pagado</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${data.data.map(item => `
                                            <tr>
                                                <td>${item.Payment_Type}</td>
                                                <td>${this.formatNumber(item.cantidad)}</td>
                                                <td>$${this.formatNumber(item.total_pagado)}</td>
                                            </tr>
                                        `).join('')}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;

        $('#pagos-content').html(html);
        Plotly.newPlot('chart-pagos', data.chart.data, data.chart.layout);
    }

    formatNumber(num) {
        return new Intl.NumberFormat('es-MX').format(num.toFixed(2));
    }
}

// Inicializar dashboard cuando el documento esté listo
document.addEventListener('DOMContentLoaded', () => {
    new Dashboard();
});