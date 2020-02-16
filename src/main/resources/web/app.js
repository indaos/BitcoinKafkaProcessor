
'use strict';

  var app = angular.module('BitсoinProcessor', []);
  app.controller('BitсoinProcessorController', function ($scope,$http) {
    var last_key=0;
    var mychart = Highcharts.chart('datachart', {
      chart: {
        type: 'column',
        events : {
           load: setInterval( function() {
             $http.get("/bitcapi/aggr-by-days",{params: {"start":last_key}})
                    .then(function(response) {
                      if (response.data.length > 0) {
                        last_key = response.data[0].key;
                        var shift = mychart.series[0].data.length > 15;
                        mychart.series[0].addPoint(
                            [response.data[0].key, response.data[0].value], true,
                            shift);
                      }
                  },function (reason) {
                    });

          },10000)
        }
      },
      title: {
        text: 'Bitcoin \n'
            + 'daily volume'
      },
      xAxis: {
        type: 'datetime',
        tickPixelInterval: 50,
        maxZoom: 20 * 1000,
        labels: {
          format: '{value:%Y-%b-%e %H:%M}'
        }
      },
      yAxis: {
        minPadding: 0.2,
        maxPadding: 0.2,
        title: {
          text: 'volume',
          margin: 80
        }
      },
      series: [{
        name: 'Bitcoins',
        data: []}
      ]
    });
  });

