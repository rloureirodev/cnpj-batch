package br.com.simplificarest.cnpjbatch.mappingcsv;
public class CsvColumnMappings {

    public static final String[] CNAE = {"codigo", "descricao"};
    public static final String[] EMPRESA = {
        "cnpjBasico",
        "razaoSocial",
        "naturezaJuridica",
        "qualificacaoResponsavel",
        "capitalSocial",
        "porte",
        "enteFederativoResponsavel"
    };
    public static final String[] ESTABELECIMENTO = {
        "cnpjBasico","cnpjOrdem","cnpjDv","identificadorMatrizFilial","nomeFantasia",
        "situacaoCadastral","dataSituacaoCadastral","motivoSituacaoCadastral",
        "nomeCidadeExterior","pais","dataInicioAtividade","cnaePrincipal","cnaeSecundaria",
        "tipoLogradouro","logradouro","numero","complemento","bairro","cep","uf","municipio",
        "ddd1","telefone1","ddd2","telefone2","dddFax","fax","email","situacaoEspecial",
        "dataSituacaoEspecial"
    };
    public static final String[] MOTIVO = {"codigo","descricao"};
    public static final String[] MUNICIPIO = {"codigo","descricao"};
    public static final String[] NATUREZA_JURIDICA = {"codigo","descricao"};
    public static final String[] PAIS = {"codigo","descricao"};
    public static final String[] QUALIFICACAO = {"codigo","descricao"};
    public static final String[] SIMPLES = {
        "cnpjBasico","opcaoSimples","dataOpcaoSimples","dataExclusaoSimples",
        "opcaoMei","dataOpcaoMei","dataExclusaoMei"
    };
    public static final String[] SOCIO = {
        "cnpjBasico","identificadorSocio","nomeSocio","cpfCnpjSocio","qualificacaoSocio",
        "dataEntradaSociedade","pais","cpfRepresentanteLegal","nomeRepresentante",
        "qualificacaoRepresentante","faixaEtaria"
    };
}
