package ru.test.services.impl;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.domain.*;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;
import ru.test.model.BranchSummary;
import ru.test.model.BranchSummaryByDate;
import ru.test.model.ClaimDataEntity;
import ru.test.model.ClaimInspectionEntity;
import ru.test.repositories.ClaimDataRepository;
import ru.test.repositories.ClaimInspectionRepository;
import ru.test.services.BaseService;
import ru.test.services.ClaimDataService;
import ru.test.common.utilities.rest.RestPageImpl;
import ru.test.model.ClaimInfo;
import ru.test.infrastructure.transport.client.MicroServiceExchangeTemplate;
import ru.test.infrastructure.transport.client.MicroServiceExchangeTemplateFactory;
import ru.test.infrastructure.transport.client.ResponsePromise;

import javax.persistence.EntityManager;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;


/**
 * Сервис обработки заявок
 *
 */

@Component
public class ClaimDataServiceImpl extends BaseService<ClaimDataEntity> implements ClaimDataService {

    private final ClaimDataRepository claimDataRepository;
	private final ProductFilterRepository productFilterRepository;
    private final ClaimInspectionRepository claimInspectionRepository;
    private final MicroServiceExchangeTemplateFactory microServiceExchangeTemplateFactory;
    private static final long DEFAULT_TIMEOUT = 20L;
    private static final String DEFAULT_APPLICATION_NAME = "claim";
    private final EntityManager entityManager;

    /**
     * Конструктор
     *
     * @param repository - репозиторий для работы с сущностью
     */
    public ClaimDataServiceImpl(ClaimDataRepository repository,
                                MicroServiceExchangeTemplateFactory microServiceExchangeTemplateFactory,
                                ClaimInspectionRepository claimInspectionRepository,
                                EntityManager entityManager) {
        super(repository);
        this.claimDataRepository = repository;
        this.microServiceExchangeTemplateFactory = microServiceExchangeTemplateFactory;
        this.claimInspectionRepository = claimInspectionRepository;
        this.entityManager = entityManager;
    }


    /**
     * Загрузить заявки из МС
     *
     */
    @Override
    public void loadClaims(LocalDate period) {

        //loadDataCountAllClaims(period);
        сalculationCountClaimToInspection(period);
        loadDataClaimToInspection(period);
    }

    protected void loadDataClaimToInspection(LocalDate period){
        List<ClaimDataEntity> dataClaims =  entityManager.createQuery(
                "SELECT data FROM ru.test.inspection.model.ClaimDataEntity data" +
                        " WHERE YEAR(data.registrationDate)=:year AND MONTH(data.registrationDate)=:month" +
                        " ORDER BY data.branchId, data.registrationDate")
                .setParameter("year", period.getYear())
                .setParameter("month", period.getMonthValue())
                .getResultList();

        for (ClaimDataEntity item : dataClaims) {
            RestPageImpl<ClaimInfo> pageClaims = getPageDataClaim(
                    new PageRequest(0, item.getCountClaimsToLoad().intValue()),
                    item.getRegistrationDate(),
                    item.getBranchId(),
                    item.getProductId());

            for(ClaimInfo claimInfo : pageClaims){
                addClaimToInspectionRecord(claimInfo);
            }
        }

    }

    protected void сalculationCountClaimToInspection(LocalDate period) {
        BranchSummary currentBranchSummary = null;
        BranchSummaryByDate currentBranchSummaryByDate = null;
        ArrayList<BranchSummary> branchSummary = new ArrayList<>();
        List<ClaimDataEntity> dataClaims =  entityManager.getClaims();

        // Рассчитаем итоги.
        for (ClaimDataEntity item : dataClaims) {
            if (currentBranchSummary == null ||
                    !currentBranchSummary.getBranch().equals(item.getBranchId())) {
                currentBranchSummary = new BranchSummary(item.getBranchId(), item.getCountClaims());
                branchSummary.add(currentBranchSummary);
            }
            else {
                currentBranchSummary.setCount(currentBranchSummary.getCount() + item.getCountClaims());
            }
            currentBranchSummaryByDate.getRows().add(item);
        }

        // Выполним расчет количества заявок к загрузке
        Long resultCount = 0L;
        for (BranchSummary itemBranch : branchSummary) {
            for (BranchSummaryByDate itemBranchByDate : itemBranch.getRows()) {
                for (ClaimDataEntity itemProduct : itemBranchByDate.getRows()) {
                    resultCount = Math.round(itemBranch.getCount() * 0.5 *
                             itemProduct.getCountClaims() / (double) itemBranchByDate.getCount() *
                             itemBranchByDate.getCount() / (double) itemBranch.getCount());

                    //Запишем результат
                    saveCountClaimsToLoad(itemProduct, resultCount);
                }
            }
        }

    }

    @Transactional
    protected void saveCountClaimsToLoad(ClaimDataEntity entity, Long resultCount){
        entity.setCountClaimsToLoad(resultCount);
        claimDataRepository.save(entity);
    }


    protected void loadDataCountAllClaims(LocalDate period)
    {
        LocalDate currentDayOfPeriod = period;
        LocalDate lastDayOfPeriod = period.withDayOfMonth(period.lengthOfMonth());

        for(String branchId : getBranchs()){
            for(String productCode : getProductsFilter()) {
                currentDayOfPeriod = period;
                while (currentDayOfPeriod.isBefore(lastDayOfPeriod) ||
                        currentDayOfPeriod.equals(lastDayOfPeriod)) {

                    addClaimDataRecord(
                            currentDayOfPeriod,
                            branchId,
                            productCode,
                            getDataCountClaim(currentDayOfPeriod, branchId, productCode));

                    currentDayOfPeriod = currentDayOfPeriod.plusDays(1);
                }
            }
        }
    }

}
